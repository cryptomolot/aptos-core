// Copyright © Aptos Foundation
// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use crate::{remote_state_view_service::RemoteStateViewService, ExecuteBlockCommand, RemoteExecutionRequest, RemoteExecutionResult, RemoteExecutionRequestRef, ExecuteBlockCommandRef};
use aptos_logger::{info, trace};
use aptos_secure_net::network_controller::{Message, MessageType, NetworkController};
use aptos_storage_interface::cached_state_view::CachedStateView;
use aptos_types::{
    block_executor::{
        config::BlockExecutorConfigFromOnchain, partitioner::PartitionedTransactions,
    },
    state_store::StateView,
    transaction::TransactionOutput,
    vm_status::VMStatus,
};
use aptos_vm::sharded_block_executor::{
    executor_client::{ExecutorClient, ShardedExecutionOutput},
    ShardedBlockExecutor,
};
use crossbeam_channel::{Receiver, Sender};
use once_cell::sync::{Lazy, OnceCell};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    thread,
};
use std::sync::atomic::AtomicU64;
use std::thread::JoinHandle;
use std::time::SystemTime;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSlice;
use aptos_drop_helper::DEFAULT_DROPPER;
use aptos_secure_net::grpc_network_service::outbound_rpc_helper::OutboundRpcHelper;
use aptos_secure_net::network_controller::metrics::{get_delta_time, REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER};
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use aptos_vm::sharded_block_executor::sharded_executor_service::{CmdsAndMetaDataRef, TransactionIdxAndOutput};
use crate::metrics::REMOTE_EXECUTOR_TIMER;

pub static COORDINATOR_PORT: u16 = 52200;

static REMOTE_ADDRESSES: OnceCell<Vec<SocketAddr>> = OnceCell::new();
static COORDINATOR_ADDRESS: OnceCell<SocketAddr> = OnceCell::new();

pub fn set_remote_addresses(addresses: Vec<SocketAddr>) {
    REMOTE_ADDRESSES.set(addresses).ok();
}

pub fn get_remote_addresses() -> Vec<SocketAddr> {
    match REMOTE_ADDRESSES.get() {
        Some(value) => value.clone(),
        None => vec![],
    }
}

pub fn set_coordinator_address(address: SocketAddr) {
    COORDINATOR_ADDRESS.set(address).ok();
}

pub fn get_coordinator_address() -> SocketAddr {
    match COORDINATOR_ADDRESS.get() {
        Some(value) => *value,
        None => SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), COORDINATOR_PORT),
    }
}

pub static REMOTE_SHARDED_BLOCK_EXECUTOR: Lazy<
    Arc<
        aptos_infallible::Mutex<
            ShardedBlockExecutor<CachedStateView, RemoteExecutorClient<CachedStateView>>,
        >,
    >,
> = Lazy::new(|| {
    info!("REMOTE_SHARDED_BLOCK_EXECUTOR created");
    Arc::new(aptos_infallible::Mutex::new(
        RemoteExecutorClient::create_remote_sharded_block_executor(
            get_coordinator_address(),
            get_remote_addresses(),
            None,
        ),
    ))
});

#[allow(dead_code)]
pub struct RemoteExecutorClient<S: StateView + Sync + Send + 'static> {
    // The network controller used to create channels to send and receive messages. We want the
    // network controller to be owned by the executor client so that it is alive for the entire
    // lifetime of the executor client.
    network_controller: NetworkController,
    state_view_service: Arc<RemoteStateViewService<S>>,
    // Channels to send execute block commands to the executor shards.
    command_txs: Arc<Vec<Vec<Mutex<OutboundRpcHelper>>>>,
    // Channels to receive execution results from the executor shards.
    result_rxs: Vec<Receiver<Message>>,
    // Thread pool used to pre-fetch the state values for the block in parallel and create an in-memory state view.
    thread_pool: Arc<rayon::ThreadPool>,
    cmd_tx_thread_pool: Arc<rayon::ThreadPool>,

    phantom: std::marker::PhantomData<S>,
    _join_handle: Option<thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl<S: StateView + Sync + Send + 'static> RemoteExecutorClient<S> {
    pub fn new(
        remote_shard_addresses: Vec<SocketAddr>,
        mut controller: NetworkController,
        num_threads: Option<usize>,
    ) -> Self {
        let num_threads = num_threads.unwrap_or_else(num_cpus::get) / 2;
        let thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .unwrap(),
        );
        let outbound_rpc_runtime = controller.get_outbound_rpc_runtime();
        let self_addr = controller.get_self_addr();
        let controller_mut_ref = &mut controller;
        let num_shards = remote_shard_addresses.len();
        let (command_txs, result_rxs) = remote_shard_addresses
            .iter()
            .enumerate()
            .map(|(shard_id, address)| {
                let execute_command_type = format!("execute_command_{}", shard_id);
                let execute_result_type = format!("execute_result_{}", shard_id);
                let mut command_tx = vec![];
                for _ in 0..num_threads/(2 * num_shards) {
                    command_tx.push(Mutex::new(OutboundRpcHelper::new(self_addr, *address, outbound_rpc_runtime.clone())));
                }
                let result_rx = controller_mut_ref.create_inbound_channel(execute_result_type);
                (command_tx, result_rx)
            })
            .unzip();

        let state_view_service = Arc::new(RemoteStateViewService::new(
            controller_mut_ref,
            remote_shard_addresses,
            None,
        ));

        let state_view_service_clone = state_view_service.clone();

        let join_handle = thread::Builder::new()
            .name("remote-state_view-service".to_string())
            .spawn(move || state_view_service_clone.start())
            .unwrap();

        controller.start();

        let cmd_tx_thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .thread_name(move |index| format!("rmt-exe-cli-cmd-tx-{}", index))
                .num_threads(num_cpus::get() / 4)
                .build()
                .unwrap(),
        );

        Self {
            network_controller: controller,
            state_view_service,
            _join_handle: Some(join_handle),
            command_txs: Arc::new(command_txs),
            result_rxs,
            thread_pool,
            cmd_tx_thread_pool,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn create_remote_sharded_block_executor(
        coordinator_address: SocketAddr,
        remote_shard_addresses: Vec<SocketAddr>,
        num_threads: Option<usize>,
    ) -> ShardedBlockExecutor<S, RemoteExecutorClient<S>> {
        ShardedBlockExecutor::new(RemoteExecutorClient::new(
            remote_shard_addresses,
            NetworkController::new(
                "remote-executor-coordinator".to_string(),
                coordinator_address,
                5000,
            ),
            num_threads,
        ))
    }

    fn get_output_from_shards(&self) -> Result<Vec<Vec<Vec<TransactionOutput>>>, VMStatus> {
        trace!("RemoteExecutorClient Waiting for results");
        /*let thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.num_shards())
                .build()
                .unwrap(),
        );

        let mut results = vec![];
        for rx in self.result_rxs.iter() {
            let received_bytes = rx.recv().unwrap().to_bytes();
            let result: RemoteExecutionResult = bcs::from_bytes(&received_bytes).unwrap();
            results.push(result.inner?);
        }*/

        let results: Vec<(usize, Vec<Vec<TransactionOutput>>)> = (0..self.num_shards()).into_par_iter().map(|shard_id| {
            let received_msg = self.result_rxs[shard_id].recv().unwrap();
            let delta = get_delta_time(received_msg.start_ms_since_epoch.unwrap());
            REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                .with_label_values(&["9_1_results_tx_msg_remote_exe_recv"]).observe(delta as f64);

            let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
                .with_label_values(&["0", "result_rx_bcs_deser"])
                .start_timer();
            let result: RemoteExecutionResult = bcs::from_bytes(&received_msg.to_bytes()).unwrap();
            drop(bcs_deser_timer);
            (shard_id, result.inner.unwrap())
        }).collect();

        let _timer = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "result_rx_gather"])
            .start_timer();
        let mut res: Vec<Vec<Vec<TransactionOutput>>> = vec![vec![]; self.num_shards()];
        for (shard_id, result) in results.into_iter() {
            res[shard_id] = result;
        }
        Ok(res)
    }

    // Parallel results aggregation from shards. Requires each network message to provide
    // the number of transactions in the message (seq_num variable).
    fn get_streamed_output_from_shards(&self, expected_outputs: Vec<u64>, duration_since_epoch: u64) -> Result<Vec<TransactionOutput>, VMStatus> {
        //info!("expected outputs {:?} ", expected_outputs);
        #[derive(Copy, Clone)]
        struct Pointer(*mut TransactionOutput);
        unsafe impl Send for Pointer {}
        unsafe impl Sync for Pointer {}

        let total_expected_outputs = expected_outputs.iter().sum::<u64>();
        let mut results = vec![TransactionOutput::default(); total_expected_outputs as usize];
        let results_ptr = Pointer(results.as_mut_ptr());
        let num_deser_threads = self.num_shards();
        let deser_thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .thread_name(move |index| format!("rmt-exe-cli-res-rx-{}", index))
                .num_threads(num_deser_threads)
                .build()
                .unwrap(),
        );
        let (deser_tx, deser_rx) = crossbeam_channel::unbounded();
        let (deser_finished_tx, deser_finished_rx) = crossbeam_channel::unbounded();
        let deser_finished_tx = Arc::new(deser_finished_tx);
        for _ in 0..num_deser_threads {
            let results_clone = results_ptr.clone();
            let deser_rx_clone: Receiver<Message> = deser_rx.clone();
            let deser_finished_tx_clone = deser_finished_tx.clone();
            deser_thread_pool.spawn(move || {
                while let Ok(msg) = deser_rx_clone.recv() {
                    let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
                        .with_label_values(&["0", "result_rx_bcs_deser"])
                        .start_timer();
                    let result: Vec<TransactionIdxAndOutput> = bcs::from_bytes(&msg.to_bytes()).unwrap();
                    drop(bcs_deser_timer);
                    for txn_output in result {
                        // correctness guaranteed by disjointness of txn indices
                        unsafe { *{ results_clone }.0.wrapping_add(txn_output.txn_idx as usize) = txn_output.txn_output; }
                    }
                }
                deser_finished_tx_clone.send(()).unwrap();
            });
        }
        (0..self.num_shards()).into_par_iter().for_each(|shard_id| {
            let mut num_outputs_received: u64 = 0;
            loop {
                let received_msg = self.result_rxs[shard_id].recv().unwrap();
                let num_txn = received_msg.seq_num.unwrap();
                num_outputs_received += num_txn;
                deser_tx.send(received_msg).unwrap();
                if num_outputs_received == expected_outputs[shard_id] {
                    let delta = get_delta_time(duration_since_epoch);
                    REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                        .with_label_values(&["9_1_results_tx_msg_remote_exe_recv"]).observe(delta as f64);
                    break;
                }
            }
        });
        drop(deser_tx);
        let mut cnt = 0;
        while let Ok(msg) = deser_finished_rx.recv() {
            cnt += 1;
            if cnt == num_deser_threads {
                break;
            }
        }
        let delta = get_delta_time(duration_since_epoch);
        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
            .with_label_values(&["9_2_results_rx_all_shards"]).observe(delta as f64);

        Ok(results)
    }

    // fn get_streamed_output_from_shards(&self, expected_outputs: Vec<u64>, duration_since_epoch: u64) -> Result<Vec<TransactionOutput>, VMStatus> {
    //     //info!("expected outputs {:?} ", expected_outputs);
    //     let results: Vec<Vec<TransactionIdxAndOutput>> = (0..self.num_shards()).into_par_iter().map(|shard_id| {
    //         let mut num_outputs_received: u64 = 0;
    //         let mut outputs = vec![];
    //         loop {
    //             let received_msg = self.result_rxs[shard_id].recv().unwrap();
    //             let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
    //                 .with_label_values(&["0", "result_rx_bcs_deser"])
    //                 .start_timer();
    //             let result: Vec<TransactionIdxAndOutput> = bcs::from_bytes(&received_msg.to_bytes()).unwrap();
    //             drop(bcs_deser_timer);
    //             num_outputs_received += result.len() as u64;
    //             //info!("Streamed output from shard {}; txn_id {}", shard_id, result.txn_idx);
    //             outputs.extend(result);
    //             if num_outputs_received == expected_outputs[shard_id] {
    //                 let delta = get_delta_time(duration_since_epoch);
    //                 REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
    //                     .with_label_values(&["9_1_results_tx_msg_remote_exe_recv"]).observe(delta as f64);
    //                 break;
    //             }
    //         }
    //         outputs
    //     }).collect();
    //
    //     let delta = get_delta_time(duration_since_epoch);
    //     REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
    //         .with_label_values(&["9_2_results_rx_all_shards"]).observe(delta as f64);
    //
    //     let _timer = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "result_rx_gather"])
    //         .start_timer();
    //     let mut aggregated_results: Vec<TransactionOutput> = vec![Default::default() ; expected_outputs.iter().sum::<u64>() as usize];
    //     results.into_iter().for_each(|result| {
    //         result.into_iter().for_each(|txn_output| {
    //             aggregated_results[txn_output.txn_idx as usize] = txn_output.txn_output;
    //         });
    //     });
    //
    //     Ok(aggregated_results)
    // }
}

impl<S: StateView + Sync + Send + 'static> ExecutorClient<S> for RemoteExecutorClient<S> {
    fn num_shards(&self) -> usize {
        self.command_txs.len()
    }

    fn execute_block(&self, state_view: Arc<S>, transactions: PartitionedTransactions, concurrency_level_per_shard: usize, onchain_config: BlockExecutorConfigFromOnchain) -> Result<ShardedExecutionOutput, VMStatus> {
        panic!("Not implemented for RemoteExecutorClient");
    }

    fn execute_block_remote(
        &self,
        state_view: Arc<S>,
        transactions: Arc<PartitionedTransactions>,
        concurrency_level_per_shard: usize,
        onchain_config: BlockExecutorConfigFromOnchain,
        duration_since_epoch: u64
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        trace!("RemoteExecutorClient Sending block to shards");
        self.state_view_service.set_state_view(state_view);
        let (sub_blocks, global_txns) = transactions.get_ref();
        if !global_txns.is_empty() {
            panic!("Global transactions are not supported yet");
        }

        let cmd_tx_timer = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "cmd_tx_async"])
            .start_timer();


        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
            .with_label_values(&["0_cmd_tx_start"]).observe(get_delta_time(duration_since_epoch) as f64);

        let mut expected_outputs = vec![0; self.num_shards()];
        let batch_size = 200;
        for (shard_id, _) in sub_blocks.into_iter().enumerate() {
            expected_outputs[shard_id] = transactions.get_ref().0[shard_id].num_txns() as u64;
            // TODO: Check if the function can get Arc<BlockExecutorConfigFromOnchain> instead.
            let onchain_config_clone = onchain_config.clone();
            let transactions_clone = transactions.clone();
            let senders = self.command_txs.clone();
            self.cmd_tx_thread_pool.spawn(move || {
                let shard_txns = &transactions_clone.get_ref().0[shard_id].sub_blocks[0].transactions;
                let index_offset = transactions_clone.get_ref().0[shard_id].sub_blocks[0].start_index as usize;
                let num_txns = shard_txns.len();

                let _ = shard_txns
                    .chunks(batch_size)
                    .enumerate()
                    .for_each(|(chunk_idx, txns)| {
                        let analyzed_txns = txns.iter().map(|txn| {
                            txn.txn()
                        }).collect::<Vec<&AnalyzedTransaction>>();
                        let execution_batch_req = CmdsAndMetaDataRef {
                            cmds: &analyzed_txns,
                            num_txns,
                            shard_txns_start_index: index_offset,
                            onchain_config: &onchain_config_clone,
                            batch_start_index: chunk_idx * batch_size,
                        };
                        let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
                            .with_label_values(&["0", "cmd_tx_bcs_ser"])
                            .start_timer();
                        let msg = Message::create_with_metadata(bcs::to_bytes(&execution_batch_req).unwrap(), duration_since_epoch, analyzed_txns.len() as u64, 0);
                        drop(bcs_ser_timer);
                        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                            .with_label_values(&["1_cmd_tx_msg_send"]).observe(get_delta_time(duration_since_epoch) as f64);
                        let execute_command_type = format!("execute_command_{}", shard_id);
                        let mut rng = StdRng::from_entropy();
                        let rand_send_thread_idx = rng.gen_range(0, senders[shard_id].len());
                        senders[shard_id][rand_send_thread_idx]
                            .lock()
                            .unwrap()
                            .send(msg, &MessageType::new(execute_command_type));
                    });
                
                /*let shard_txns = &transactions_clone.get_ref().0[shard_id];
                let index_offset = shard_txns.sub_blocks[0].start_index as usize;
                let num_txns = shard_txns.num_txns();
                let mut batch_start_idx = 0;

                for batch in &shard_txns.iter().chunks(batch_size) {
                    let senders = self.command_txs.clone();
                    cmd_tx_thread_pool_clone.spawn(move || {
                        let analyzed_txns = batch.map(|txn| {
                            txn.txn()
                        }).collect::<Vec<&AnalyzedTransaction>>();
                        let execution_batch_req = CmdsAndMetaDataRef {
                            cmds: &analyzed_txns,
                            num_txns,
                            shard_txns_start_index: index_offset,
                            batch_start_index: batch_start_idx,
                        };
                        let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
                            .with_label_values(&["0", "cmd_tx_bcs_ser"])
                            .start_timer();
                        let msg = Message::create_with_metadata(bcs::to_bytes(&execution_batch_req).unwrap(), duration_since_epoch, 0, 0);
                        drop(bcs_ser_timer);
                        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                            .with_label_values(&["1_cmd_tx_msg_send"]).observe(get_delta_time(duration_since_epoch) as f64);
                        let execute_command_type = format!("execute_command_{}", shard_id);
                        senders[shard_id]
                            .lock()
                            .unwrap()
                            .send(msg, &MessageType::new(execute_command_type));
                    });*/


                /*let analyzed_txns = &transactions_clone.get_ref().0[shard_id]
                    .iter()
                    .enumerate()
                    .map(|(idx, txn)| {
                        (txn.txn(), idx)
                    }).collect::<Vec<(&AnalyzedTransaction, usize)>>();

                let mut st_idx = 0;

                while st_idx < num_txns {
                    let end_idx = std::cmp::min(st_idx + batch_size, num_txns);
                    let execution_batch_req = CmdsAndMetaDataRef {
                        cmds: &analyzed_txns[st_idx..end_idx],
                        num_txns,
                        shard_txns_start_index: index_offset,
                        onchain_config: &onchain_config_clone,
                    };
                    let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
                        .with_label_values(&["0", "cmd_tx_bcs_ser"])
                        .start_timer();
                    let msg = Message::create_with_metadata(bcs::to_bytes(&execution_batch_req).unwrap(), duration_since_epoch, 0, 0);
                    drop(bcs_ser_timer);
                    REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                        .with_label_values(&["1_cmd_tx_msg_send"]).observe(get_delta_time(duration_since_epoch) as f64);
                    let execute_command_type = format!("execute_command_{}", shard_id);
                    senders[shard_id]
                        .lock()
                        .unwrap()
                        .send(msg, &MessageType::new(execute_command_type));
                    st_idx += batch_size;
                }

                let execution_request = RemoteExecutionRequestRef::ExecuteBlock(ExecuteBlockCommandRef {
                    sub_blocks: &transactions_clone.get_ref().0[shard_id],
                    concurrency_level: concurrency_level_per_shard,
                    onchain_config: &onchain_config_clone,
                });

                let execute_command_type = format!("execute_command_{}", shard_id);
                let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
                    .with_label_values(&["0", "cmd_tx_bcs_ser"])
                    .start_timer();
                let msg = Message::create_with_metadata(bcs::to_bytes(&execution_request).unwrap(), duration_since_epoch, 0, 0);
                drop(bcs_ser_timer);
                REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                    .with_label_values(&["1_cmd_tx_msg_send"]).observe(get_delta_time(duration_since_epoch) as f64);
                senders[shard_id]
                    .lock()
                    .unwrap()
                    .send(msg, &MessageType::new(execute_command_type));*/
            });
        }

        drop(cmd_tx_timer);

        //let execution_results = self.get_output_from_shards()?;

        let results = self.get_streamed_output_from_shards(expected_outputs, duration_since_epoch);

        let timer = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "drop_state_view_finally"])
            .start_timer();
        self.state_view_service.drop_state_view();
        drop(timer);
        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
            .with_label_values(&["9_8_execute_remote_block_done"]).observe(get_delta_time(duration_since_epoch) as f64);
        drop(transactions);
        //DEFAULT_DROPPER.schedule_drop(transactions);
        results
        //Ok(ShardedExecutionOutput::new(execution_results, vec![]))
    }

    fn shutdown(&mut self) {
        self.network_controller.shutdown();
    }
}
