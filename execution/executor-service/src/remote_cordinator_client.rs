// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use crate::{
    metrics::REMOTE_EXECUTOR_TIMER, remote_state_view::RemoteStateViewClient, ExecuteBlockCommand,
    RemoteExecutionRequest, RemoteExecutionResult,
};
use aptos_secure_net::network_controller::{Message, MessageType, NetworkController, OutboundRpcScheduler};
use aptos_types::{
    block_executor::partitioner::ShardId, state_store::state_key::StateKey,
    transaction::TransactionOutput, vm_status::VMStatus,
};
use aptos_vm::sharded_block_executor::{coordinator_client::CoordinatorClient, ExecutorShardCommand, StreamedExecutorShardCommand};
use crossbeam_channel::{Receiver, Sender};
use rayon::prelude::*;
use std::{net::SocketAddr, sync::Arc, thread};
use std::ops::AddAssign;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Mutex;
use std::time::SystemTime;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use aptos_infallible::duration_since_epoch;
use aptos_logger::info;
use aptos_secure_net::grpc_network_service::outbound_rpc_helper::OutboundRpcHelper;
use aptos_secure_net::network_controller::metrics::{get_delta_time, REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER};
use aptos_types::transaction::analyzed_transaction::AnalyzedTransaction;
use aptos_vm::sharded_block_executor::sharded_executor_service::{CmdsAndMetaData, TransactionIdxAndOutput};
use aptos_vm::sharded_block_executor::streamed_transactions_provider::BlockingTransactionsProvider;

pub struct RemoteCoordinatorClient {
    state_view_client: Arc<RemoteStateViewClient>,
    command_rx: Arc<Receiver<Message>>,
    //result_tx: Sender<Message>,
    result_tx: Arc<Vec<Arc<tokio::sync::Mutex<OutboundRpcHelper>>>>,
    shard_id: ShardId,
    cmd_rx_msg_duration_since_epoch: Arc<AtomicU64>,
    is_block_init_done: Arc<AtomicBool>,//Mutex<bool>,
    cmd_rx_thread_pool: Arc<rayon::ThreadPool>,
    result_tx_thread_pool: Arc<rayon::ThreadPool>,
    outbound_rpc_scheduler: Arc<OutboundRpcScheduler>,
}

impl RemoteCoordinatorClient {
    pub fn new(
        shard_id: ShardId,
        controller: &mut NetworkController,
        coordinator_address: SocketAddr,
    ) -> Self {
        let execute_command_type = format!("execute_command_{}", shard_id);
        let execute_result_type = format!("execute_result_{}", shard_id);
        let command_rx = controller.create_inbound_channel(execute_command_type);
        let mut result_tx = vec![];
        for _ in 0..4 {
            result_tx.push(Arc::new(tokio::sync::Mutex::new(OutboundRpcHelper::new(controller.get_self_addr(), coordinator_address, controller.get_outbound_rpc_runtime()))));
        }
            //controller.create_outbound_channel(coordinator_address, execute_result_type);
        let cmd_rx_thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .thread_name(move |index| format!("remote-state-view-shard-send-request-{}-{}", shard_id, index))
                .num_threads(16)
                .build()
                .unwrap(),
        );

        let result_tx_thread_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .thread_name(move |index| format!("remote-state-view-shard-send-request-{}-{}", shard_id, index))
                .num_threads(4)
                .build()
                .unwrap(),
        );

        let state_view_client =
            RemoteStateViewClient::new(shard_id, controller, coordinator_address);

        let outbound_rpc_scheduler = controller.get_outbound_rpc_scheduler();
        Self {
            state_view_client: Arc::new(state_view_client),
            command_rx: Arc::new(command_rx),
            result_tx: Arc::new(result_tx),
            shard_id,
            cmd_rx_msg_duration_since_epoch: Arc::new(AtomicU64::new(0)),
            is_block_init_done: Arc::new(AtomicBool::new(false)),
            cmd_rx_thread_pool,
            result_tx_thread_pool,
            outbound_rpc_scheduler,
        }
    }

    // Extract all the state keys from the execute block command. It is possible that there are duplicate state keys.
    // We are not de-duplicating them here to avoid the overhead of deduplication. The state view server will deduplicate
    // the state keys.
    fn extract_state_keys(command: &ExecuteBlockCommand) -> Vec<StateKey> {
        command
            .sub_blocks
            .sub_block_iter()
            .flat_map(|sub_block| {
                sub_block
                    .transactions
                    .iter()
                    .map(|txn| {
                        let mut state_keys = vec![];
                        for storage_location in txn
                            .txn()
                            .read_hints()
                            .iter()
                            .chain(txn.txn().write_hints().iter())
                        {
                            state_keys.push(storage_location.state_key().clone());
                        }
                        state_keys
                    })
                    .flatten()
                    .collect::<Vec<StateKey>>()
            })
            .collect::<Vec<StateKey>>()
    }

    fn extract_state_keys_from_txns(txns: &Vec<AnalyzedTransaction>) -> Vec<StateKey> {
        let mut state_keys = vec![];
        for txn in txns {
            for storage_location in txn
                .read_hints()
                .iter()
                .chain(txn.write_hints().iter())
            {
                state_keys.push(storage_location.state_key().clone());
            }
        }
        state_keys
    }

    fn receive_execute_command_stream_follow_up(state_view_client: Arc<RemoteStateViewClient>,
                                                command_rx: Arc<Receiver<Message>>,
                                                blocking_transactions_provider: Arc<BlockingTransactionsProvider>,
                                                num_txns_in_the_block: usize,
                                                mut num_txns_processed: usize,
                                                shard_id: ShardId,
                                                cmd_rx_msg_duration_since_epoch: Arc<AtomicU64>,
                                                is_block_init_done: Arc<AtomicBool>,
                                                cmd_rx_thread_pool: Arc<rayon::ThreadPool>,) {
        if num_txns_processed == num_txns_in_the_block {
            //info!("Breaking out initially .............................");
            return;
        }
        let mut rng = StdRng::from_entropy();
        let mut break_out = false;
        loop {
            if break_out {
                //info!("Breaking out of the loop.............................");
                break;
            }
            match command_rx.recv() {
                Ok(message) => {
                    let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                    info!("Received cmd batch {} at time: {}", message.shard_id.unwrap(), curr_time);
                    let state_view_client_clone = state_view_client.clone();
                    let blocking_transactions_provider_clone = blocking_transactions_provider.clone();
                    let cmd_rx_msg_duration_since_epoch_clone = cmd_rx_msg_duration_since_epoch.clone();
                    let is_block_init_done_clone = is_block_init_done.clone();
                    let cmd_rx_thread_pool_clone = cmd_rx_thread_pool.clone();

                    num_txns_processed += message.seq_num.unwrap() as usize;
                    info!("txns considered is ********* {}; num txns in block {}", num_txns_processed, num_txns_in_the_block);
                    if num_txns_processed == num_txns_in_the_block {
                        is_block_init_done_clone.store(false, std::sync::atomic::Ordering::Relaxed);
                        break_out = true;
                    }
                    let random_number = rng.gen_range(0, u64::MAX);
                    cmd_rx_thread_pool_clone.spawn_fifo(move || {
                        let delta = get_delta_time(message.start_ms_since_epoch.unwrap());
                        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                            .with_label_values(&["5_cmd_tx_msg_shard_recv"]).observe(delta as f64);
                        cmd_rx_msg_duration_since_epoch_clone.store(message.start_ms_since_epoch.unwrap(), std::sync::atomic::Ordering::Relaxed);
                        let _rx_timer = REMOTE_EXECUTOR_TIMER
                            .with_label_values(&[&shard_id.to_string(), "cmd_rx"])
                            .start_timer();
                        let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
                            .with_label_values(&[&shard_id.to_string(), "cmd_rx_bcs_deser"])
                            .start_timer();
                        let txns: CmdsAndMetaData = bcs::from_bytes(&message.data).unwrap();
                        drop(bcs_deser_timer);

                        let transactions = txns.cmds;

                        let init_prefetch_timer = REMOTE_EXECUTOR_TIMER
                            .with_label_values(&[&shard_id.to_string(), "init_prefetch"])
                            .start_timer();


                        let batch_start_index = txns.batch_start_index;
                        let state_keys = Self::extract_state_keys_from_txns(&transactions);

                        state_view_client_clone.pre_fetch_state_values(state_keys, false, random_number, message.shard_id.unwrap());

                        let _ = transactions.into_iter().enumerate().for_each(|(idx, txn)| {
                            blocking_transactions_provider_clone.set_txn(idx + batch_start_index, txn);
                        });
                    });
                },
                Err(_) => { break; }
            }
        }
    }
}

impl CoordinatorClient<RemoteStateViewClient> for RemoteCoordinatorClient {
    fn receive_execute_command(&self) -> ExecutorShardCommand<RemoteStateViewClient> {
        match self.command_rx.recv() {
            Ok(message) => {
                let delta = get_delta_time(message.start_ms_since_epoch.unwrap());
                REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                    .with_label_values(&["5_cmd_tx_msg_shard_recv"]).observe(delta as f64);
                self.cmd_rx_msg_duration_since_epoch.store(message.start_ms_since_epoch.unwrap(), std::sync::atomic::Ordering::Relaxed);
                let _rx_timer = REMOTE_EXECUTOR_TIMER
                    .with_label_values(&[&self.shard_id.to_string(), "cmd_rx"])
                    .start_timer();
                let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
                    .with_label_values(&[&self.shard_id.to_string(), "cmd_rx_bcs_deser"])
                    .start_timer();
                let request: RemoteExecutionRequest = bcs::from_bytes(&message.data).unwrap();
                drop(bcs_deser_timer);

                match request {
                    RemoteExecutionRequest::ExecuteBlock(command) => {
                        let init_prefetch_timer = REMOTE_EXECUTOR_TIMER
                            .with_label_values(&[&self.shard_id.to_string(), "init_prefetch"])
                            .start_timer();
                        let state_keys = Self::extract_state_keys(&command);
                        self.state_view_client.init_for_block();
                        self.state_view_client.pre_fetch_state_values(state_keys, false, 0, 1);
                        drop(init_prefetch_timer);

                        let (sub_blocks, concurrency, onchain_config) = command.into();
                        ExecutorShardCommand::ExecuteSubBlocks(
                            self.state_view_client.clone(),
                            sub_blocks,
                            concurrency,
                            onchain_config,
                        )
                    },
                }
            },
            Err(_) => ExecutorShardCommand::Stop,
        }
    }

    fn receive_execute_command_stream(&self) -> StreamedExecutorShardCommand<RemoteStateViewClient> {
        match self.command_rx.recv() {
            Ok(message) => {
                let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                info!("Received cmd batch {} at time: {}", 200, curr_time);

                let delta = get_delta_time(message.start_ms_since_epoch.unwrap());
                REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                    .with_label_values(&["5_cmd_tx_msg_shard_recv"]).observe(delta as f64);
                self.cmd_rx_msg_duration_since_epoch.store(message.start_ms_since_epoch.unwrap(), std::sync::atomic::Ordering::Relaxed);
                let _rx_timer = REMOTE_EXECUTOR_TIMER
                    .with_label_values(&[&self.shard_id.to_string(), "cmd_rx"])
                    .start_timer();
                let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
                    .with_label_values(&[&self.shard_id.to_string(), "cmd_rx_bcs_deser"])
                    .start_timer();
                let txns: CmdsAndMetaData = bcs::from_bytes(&message.data).unwrap();
                drop(bcs_deser_timer);
                let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                info!("Checkpoint 1.1 at time: {}", curr_time);

                let init_prefetch_timer = REMOTE_EXECUTOR_TIMER
                    .with_label_values(&[&self.shard_id.to_string(), "init_prefetch"])
                    .start_timer();

                self.state_view_client.init_for_block();
                let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                info!("Checkpoint 1.2 at time: {}", curr_time);
                let state_keys = Self::extract_state_keys_from_txns(&txns.cmds);
                self.state_view_client.pre_fetch_state_values(state_keys, true, 0, 1);
                let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                info!("Checkpoint 1.3 at time: {}", curr_time);
                let num_txns = txns.num_txns;
                let num_txns_in_the_batch = txns.cmds.len();
                let shard_txns_start_index = txns.shard_txns_start_index;
                let batch_start_index = txns.batch_start_index;
                self.is_block_init_done.store(true, std::sync::atomic::Ordering::Relaxed);
                let blocking_transactions_provider = Arc::new(BlockingTransactionsProvider::new(num_txns));
                let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                info!("Checkpoint 1.4 at time: {}", curr_time);

                let command_rx = self.command_rx.clone();
                let blocking_transactions_provider_clone = blocking_transactions_provider.clone();
                let shard_id = self.shard_id;
                let cmd_rx_msg_duration_since_epoch_clone = self.cmd_rx_msg_duration_since_epoch.clone();
                let state_view_client_clone = self.state_view_client.clone();
                let is_block_init_done_clone = self.is_block_init_done.clone();
                let cmd_rx_thread_pool_clone = self.cmd_rx_thread_pool.clone();
                self.cmd_rx_thread_pool.spawn_fifo(move || {
                    Self::receive_execute_command_stream_follow_up(
                        state_view_client_clone,
                        command_rx,
                        blocking_transactions_provider_clone,
                        num_txns,
                        num_txns_in_the_batch,
                        shard_id,
                        cmd_rx_msg_duration_since_epoch_clone,
                        is_block_init_done_clone,
                        cmd_rx_thread_pool_clone);
                });

                return StreamedExecutorShardCommand::InitBatch(
                    self.state_view_client.clone(),
                    txns.cmds,
                    num_txns,
                    shard_txns_start_index,
                    txns.onchain_config,
                    batch_start_index,
                    blocking_transactions_provider);
            },
            Err(_) => StreamedExecutorShardCommand::Stop,
        }
    }

    fn reset_block_init(&self) {
        self.is_block_init_done.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    fn send_execution_result(&mut self, result: Result<Vec<Vec<TransactionOutput>>, VMStatus>) {
        unimplemented!("send_execution_result not implemented");
        // let execute_result_type = format!("execute_result_{}", self.shard_id);
        // let duration_since_epoch = self.cmd_rx_msg_duration_since_epoch.load(std::sync::atomic::Ordering::Relaxed);
        // let remote_execution_result = RemoteExecutionResult::new(result);
        // let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
        //     .with_label_values(&[&self.shard_id.to_string(), "result_tx_bcs_ser"])
        //     .start_timer();
        // let output_message = bcs::to_bytes(&remote_execution_result).unwrap();
        // drop(bcs_ser_timer);
        // let delta = get_delta_time(duration_since_epoch);
        // REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
        //     .with_label_values(&["6_results_tx_msg_shard_send"]).observe(delta as f64);
        // self.result_tx[0].lock().unwrap().send(Message::create_with_metadata(output_message, duration_since_epoch, 0, 0), &MessageType::new(execute_result_type));
    }

    fn stream_execution_result(&mut self, txn_idx_output: Vec<TransactionIdxAndOutput>, rand_result_rx_thread: usize, seq_num: u64) {
        //info!("Sending output to coordinator for txn_idx: {:?}", txn_idx_output.txn_idx);
        let result_tx_clone = self.result_tx.clone();
        let shard_id_clone = self.shard_id.clone();
        let outbound_rpc_scheduler_clone = self.outbound_rpc_scheduler.clone();
        let num_txn = txn_idx_output.len();
        self.result_tx_thread_pool.spawn(move || {
            let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
                .with_label_values(&[&shard_id_clone.to_string(), "result_tx_bcs_ser"])
                .start_timer();
            let execute_result_type = format!("execute_result_{}", shard_id_clone);
            let output_message = bcs::to_bytes(&txn_idx_output).unwrap();
            drop(bcs_ser_timer);
            let tx_send_timer = REMOTE_EXECUTOR_TIMER
                .with_label_values(&[&shard_id_clone.to_string(), "result_tx_send"])
                .start_timer();
            outbound_rpc_scheduler_clone.send(
                Message::create_with_metadata(output_message, 0, seq_num, num_txn as u64),
                MessageType::new(execute_result_type),
                result_tx_clone[rand_result_rx_thread % result_tx_clone.len()].clone(),
                seq_num);
            // result_tx_clone[rand_result_rx_thread].lock().unwrap().send(Message::new(output_message), &MessageType::new(execute_result_type));
            let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
            info!("Sent cmd results batch at time: {}", curr_time);
        });
    }

    fn record_execution_complete_time_on_shard(&self) {
        let duration_since_epoch = self.cmd_rx_msg_duration_since_epoch.load(std::sync::atomic::Ordering::Relaxed);
        let delta = get_delta_time(duration_since_epoch);
        REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
            .with_label_values(&["6_exe_complete_on_shard"]).observe(delta as f64);
    }

    fn reset_state_view(&self) {
        self.state_view_client.init_for_block();
    }
}
