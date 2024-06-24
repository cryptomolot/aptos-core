// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use crate::{RemoteKVRequest, RemoteKVResponse};
use aptos_secure_net::network_controller::{Message, MessageType, NetworkController};
use crossbeam_channel::{Receiver, Sender, unbounded};
use std::{net::SocketAddr, sync::{Arc, RwLock}, thread};
use std::ops::DerefMut;
use std::time::SystemTime;

extern crate itertools;
use crate::metrics::REMOTE_EXECUTOR_TIMER;
use aptos_logger::{info, trace};
use aptos_types::state_store::{StateView, TStateView};
use itertools::Itertools;
use std::sync::{Condvar, Mutex};
use cpq::ConcurrentPriorityQueue;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use aptos_drop_helper::DEFAULT_DROPPER;
use aptos_secure_net::grpc_network_service::outbound_rpc_helper::OutboundRpcHelper;
use aptos_secure_net::network_controller::metrics::REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER;


pub struct RemoteStateViewService<S: StateView + Sync + Send + 'static> {
    kv_rx: Receiver<Message>,
    kv_unprocessed_pq: Arc<ConcurrentPriorityQueue<Message, u64>>,
    kv_tx: Arc<Vec<Vec<Mutex<OutboundRpcHelper>>>>,
    //thread_pool: Arc<Vec<rayon::ThreadPool>>,
    thread_pool: Arc<rayon::ThreadPool>,
    state_view: Arc<RwLock<Option<Arc<S>>>>,
    recv_condition: Arc<(Mutex<bool>, Condvar)>,
    // new
    kv_req_process_thread_pool: Arc<rayon::ThreadPool>,
    kv_cmd_rx_pool: Vec<Arc<Receiver<Message>>>,
    kv_cmd_tx_pool: Vec<Sender<Message>>,
    num_kv_req_threads: usize,
}

impl<S: StateView + Sync + Send + 'static> RemoteStateViewService<S> {
    pub fn new(
        controller: &mut NetworkController,
        remote_shard_addresses: Vec<SocketAddr>,
        num_threads: Option<usize>,
    ) -> Self {
        let num_threads = num_threads.unwrap_or_else(num_cpus::get);
        let num_kv_req_threads = remote_shard_addresses.len(); //num_cpus::get() / 2;
        let num_shards = remote_shard_addresses.len();
        info!("num threads for remote state view service: {}", num_threads);
        /*let mut thread_pool = vec![];
        for _ in 0..remote_shard_addresses.len() {
            thread_pool.push(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(num_threads)
                    .thread_name(|i| format!("remote-state-view-service-kv-request-handler-{}", i))
                    .build()
                    .unwrap(),
            );
        }*/
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_kv_req_threads)
            .thread_name(|i| format!("remote-state-view-service-kv-request-handler-{}", i))
            .build()
            .unwrap();
        // let thread_pool = rayon::ThreadPoolBuilder::new()
        //     .num_threads(num_threads)
        //     .thread_name(|i| format!("remote-state-view-service-kv-request-handler-{}", i))
        //     .build()
        //     .unwrap();
        let kv_request_type = "remote_kv_request";
        let kv_response_type = "remote_kv_response";
        let result_rx = controller.create_inbound_channel(kv_request_type.to_string());
        let command_txs = remote_shard_addresses
            .iter()
            .map(|address| {
                //controller.create_outbound_channel(*address, kv_response_type.to_string())
                let mut command_tx = vec![];
                for _ in 0..num_kv_req_threads {
                    command_tx.push(Mutex::new(OutboundRpcHelper::new(controller.get_self_addr(), *address, controller.get_outbound_rpc_runtime())));
                }
                command_tx
            })
            .collect_vec();

        let num_kv_req_process_threads = 45;
        //maybe tokio here
        let kv_req_process_thread_pool = Arc::new(rayon::ThreadPoolBuilder::new()
            .num_threads(num_kv_req_threads)
            .thread_name(|i| format!("remote-state-view-service-kv-request-handler-{}", i))
            .build()
            .unwrap());
        let (kv_cmd_tx_pool, kv_cmd_rx_pool): (Vec<Sender<Message>>, Vec<Arc<Receiver<Message>>>) = (0..num_kv_req_threads)
            .map(|_| {
                let (tx, rx) = unbounded();
                (tx, Arc::new(rx))
            })
            .unzip();
        Self {
            kv_rx: result_rx,
            kv_unprocessed_pq: Arc::new(ConcurrentPriorityQueue::new()),
            kv_tx: Arc::new(command_txs),
            thread_pool: Arc::new(thread_pool),
            state_view: Arc::new(RwLock::new(None)),
            recv_condition: Arc::new((Mutex::new(false), Condvar::new())),
            kv_req_process_thread_pool,
            kv_cmd_rx_pool,
            kv_cmd_tx_pool,
            num_kv_req_threads,
        }
    }

    pub fn set_state_view(&self, state_view: Arc<S>) {
        let mut state_view_lock = self.state_view.write().unwrap();
        *state_view_lock = Some(state_view);
    }

    pub fn drop_state_view(&self) {
        let mut state_view_lock = self.state_view.write().unwrap();
        *state_view_lock = None;
    }
    #[tokio::main]
    pub async fn start(&self) {
        //let (signal_tx, signal_rx) = unbounded();
        let thread_pool_clone = self.thread_pool.clone();

        info!("Num handlers created is {}", thread_pool_clone.current_num_threads());
        // for i in 0..thread_pool_clone.current_num_threads() {
        //     let state_view_clone = self.state_view.clone();
        //     let kv_tx_clone = self.kv_tx.clone();
        //     let kv_cmd_rx_clone = self.kv_cmd_rx_pool[i].clone();
        //     let kv_req_proc_thread_pool_clone = self.kv_req_process_thread_pool.clone();
        //     // let kv_unprocessed_pq_clone = self.kv_unprocessed_pq.clone();
        //     // let recv_condition_clone = self.recv_condition.clone();
        //
        //     thread_pool_clone
        //         .spawn(move || Self::priority_handler(state_view_clone.clone(),
        //                                               kv_tx_clone.clone(),
        //                                               kv_cmd_rx_clone));
        // }

        // for _ in 0..thread_pool_clone.current_num_threads() {
        //     let state_view_clone = self.state_view.clone();
        //     let kv_tx_clone = self.kv_tx.clone();
        //     let kv_unprocessed_pq_clone = self.kv_unprocessed_pq.clone();
        //     let recv_condition_clone = self.recv_condition.clone();
        //     thread_pool_clone
        //         .spawn(move || Self::priority_handler(state_view_clone.clone(),
        //                                               kv_tx_clone.clone(),
        //                                               kv_unprocessed_pq_clone.clone(),
        //                                               recv_condition_clone.clone()));
        // }
        let mut rng = StdRng::from_entropy();
        let mut handlers = vec![];
        while let Ok(message) = self.kv_rx.recv() {
            let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
            let mut delta = 0.0;
            if curr_time > message.start_ms_since_epoch.unwrap() {
                delta = (curr_time - message.start_ms_since_epoch.unwrap()) as f64;
            }
            REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                .with_label_values(&["2_kv_req_coord_grpc_recv_spawning_req_handler"]).observe(delta);

            let _timer = REMOTE_EXECUTOR_TIMER
                .with_label_values(&["0", "kv_requests_handler_timer"])
                .start_timer();

            let state_view_clone = self.state_view.clone();
            let kv_tx_clone = self.kv_tx.clone();
            // self.kv_cmd_tx_pool[rng.gen_range(0, self.num_kv_req_threads)].send(message).unwrap();

            handlers.push(tokio::task::spawn(async move {Self::handle_message(message, state_view_clone, kv_tx_clone, 0).await;}));
            //
            // thread_pool_clone
            //     .spawn(move || Self::handle_message(message, state_view_clone, kv_tx_clone, 0));

            // let priority = message.seq_num.unwrap();
            //
            // {
            //     let (lock, cvar) = &*self.recv_condition;
            //     let _lg = lock.lock().unwrap();
            //     self.kv_unprocessed_pq.push(message, priority);
            //     self.recv_condition.1.notify_all();
            // }
            // REMOTE_EXECUTOR_TIMER
            //     .with_label_values(&["0", "kv_req_pq_size"])
            //     .observe(self.kv_unprocessed_pq.len() as f64);
        }
        for handler in handlers {
            handler.await.unwrap();
        }
    }

    pub fn priority_handler(state_view: Arc<RwLock<Option<Arc<S>>>>,
                            kv_tx: Arc<Vec<Vec<Mutex<OutboundRpcHelper>>>>,
                            kv_cmd_rx: Arc<Receiver<Message>>,) {
        //let mut rng = StdRng::from_entropy();
        while let Ok(message) = kv_cmd_rx.recv() {
            // let (lock, cvar) = &*recv_condition;
            // let mut lg = lock.lock().unwrap();
            // if pq.is_empty() {
            //     lg = cvar.wait(lg).unwrap();
            // }
            // drop(lg);
            // if let Some(message) = pq.pop() {
            let state_view = state_view.clone();
            let kv_txs = kv_tx.clone();

            Self::handle_message(message, state_view, kv_txs, 0);
            // }
        }
    }


    // pub fn priority_handler(state_view: Arc<RwLock<Option<Arc<S>>>>,
    //                         kv_tx: Arc<Vec<Vec<Mutex<OutboundRpcHelper>>>>,
    //                         pq: Arc<ConcurrentPriorityQueue<Message, u64>>,
    //                         recv_condition: Arc<(Mutex<bool>, Condvar)>) {
    //     let mut rng = StdRng::from_entropy();
    //     loop {
    //         let (lock, cvar) = &*recv_condition;
    //         let mut lg = lock.lock().unwrap();
    //         if pq.is_empty() {
    //             lg = cvar.wait(lg).unwrap();
    //         }
    //         drop(lg);
    //         if let Some(message) = pq.pop() {
    //             let state_view = state_view.clone();
    //             let kv_txs = kv_tx.clone();
    //
    //             Self::handle_message(message, state_view, kv_txs, &mut rng);
    //         }
    //     }
    // }
    pub async fn handle_message(
        message: Message,
        state_view: Arc<RwLock<Option<Arc<S>>>>,
        kv_tx: Arc<Vec<Vec<Mutex<OutboundRpcHelper>>>>,
        rand_send_thread_idx: usize,
    ) {
        let start_ms_since_epoch = message.start_ms_since_epoch.unwrap();
        {
            let curr_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let mut delta = 0.0;
            if curr_time > start_ms_since_epoch {
                delta = (curr_time - start_ms_since_epoch) as f64;
            }
            REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                .with_label_values(&["3_kv_req_coord_handler_st"])
                .observe(delta);
        }
        // we don't know the shard id until we deserialize the message, so lets default it to 0
        let _timer = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_requests"])
            .start_timer();

        let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_req_deser"])
            .start_timer();
        let req: RemoteKVRequest = bcs::from_bytes(&message.data).unwrap();
        drop(bcs_deser_timer);

        let timer_2 = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_requests_2"])
            .start_timer();
        let (shard_id, state_keys) = req.into();
        trace!(
            "remote state view service - received request for shard {} with {} keys",
            shard_id,
            state_keys.len()
        );
        let resp = state_keys
            .into_iter()
            .map(|state_key| {
                let state_value = state_view
                    .read()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .get_state_value(&state_key)
                    .unwrap();
                (state_key, state_value)
            })
            .collect_vec();
        drop(timer_2);

        let timer_3 = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_requests_3"])
            .start_timer();
        let len = resp.len();
        let resp = RemoteKVResponse::new(resp);
        let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_resp_ser"])
            .start_timer();
        let resp_serialized = bcs::to_bytes(&resp).unwrap();
        drop(bcs_ser_timer);
        trace!(
            "remote state view service - sending response for shard {} with {} keys",
            shard_id,
            len
        );
        let seq_num = message.seq_num.unwrap();

        DEFAULT_DROPPER.schedule_drop(resp);
        DEFAULT_DROPPER.schedule_drop(message);
        // info!("Processing message with seq_num: {}", seq_num);
        let resp_message = Message::create_with_metadata(resp_serialized, start_ms_since_epoch, seq_num, shard_id as u64);
        drop(timer_3);

        let _timer_4 = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_requests_4"])
            .start_timer();

        kv_tx[shard_id][rand_send_thread_idx].lock().unwrap().send(resp_message, &MessageType::new("remote_kv_response".to_string()));
        {
            let curr_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let mut delta = 0.0;
            if curr_time > start_ms_since_epoch {
                delta = (curr_time - start_ms_since_epoch) as f64;
            }
            REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                .with_label_values(&["4_kv_req_coord_handler_end"])
                .observe(delta);
        }
    }
    // pub fn handle_message(
    //     message: Message,
    //     state_view: Arc<RwLock<Option<Arc<S>>>>,
    //     kv_tx: Arc<Vec<Vec<Mutex<OutboundRpcHelper>>>>,
    //     rng: &mut StdRng,
    // ) {
    //     let start_ms_since_epoch = message.start_ms_since_epoch.unwrap();
    //     {
    //         let curr_time = SystemTime::now()
    //             .duration_since(SystemTime::UNIX_EPOCH)
    //             .unwrap()
    //             .as_millis() as u64;
    //         let mut delta = 0.0;
    //         if curr_time > start_ms_since_epoch {
    //             delta = (curr_time - start_ms_since_epoch) as f64;
    //         }
    //         REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
    //             .with_label_values(&["3_kv_req_coord_handler_st"])
    //             .observe(delta);
    //     }
    //     // we don't know the shard id until we deserialize the message, so lets default it to 0
    //     let _timer = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "kv_requests"])
    //         .start_timer();
    //
    //     let bcs_deser_timer = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "kv_req_deser"])
    //         .start_timer();
    //     let req: RemoteKVRequest = bcs::from_bytes(&message.data).unwrap();
    //     drop(bcs_deser_timer);
    //
    //     let timer_2 = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "kv_requests_2"])
    //         .start_timer();
    //     let (shard_id, state_keys) = req.into();
    //     trace!(
    //         "remote state view service - received request for shard {} with {} keys",
    //         shard_id,
    //         state_keys.len()
    //     );
    //     let resp = state_keys
    //         .into_iter()
    //         .map(|state_key| {
    //             let state_value = state_view
    //                 .read()
    //                 .unwrap()
    //                 .as_ref()
    //                 .unwrap()
    //                 .get_state_value(&state_key)
    //                 .unwrap();
    //             (state_key, state_value)
    //         })
    //         .collect_vec();
    //     drop(timer_2);
    //
    //     let timer_3 = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "kv_requests_3"])
    //         .start_timer();
    //     let len = resp.len();
    //     let resp = RemoteKVResponse::new(resp);
    //     let bcs_ser_timer = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "kv_resp_ser"])
    //         .start_timer();
    //     let resp_serialized = bcs::to_bytes(&resp).unwrap();
    //     drop(bcs_ser_timer);
    //     trace!(
    //         "remote state view service - sending response for shard {} with {} keys",
    //         shard_id,
    //         len
    //     );
    //     let seq_num = message.seq_num.unwrap();
    //
    //     DEFAULT_DROPPER.schedule_drop(resp);
    //     DEFAULT_DROPPER.schedule_drop(message);
    //    // info!("Processing message with seq_num: {}", seq_num);
    //     let resp_message = Message::create_with_metadata(resp_serialized, start_ms_since_epoch, seq_num, shard_id as u64);
    //     drop(timer_3);
    //
    //     let _timer_4 = REMOTE_EXECUTOR_TIMER
    //         .with_label_values(&["0", "kv_requests_4"])
    //         .start_timer();
    //
    //     let rand_send_thread_idx = rng.gen_range(0, kv_tx[shard_id].len());
    //     kv_tx[shard_id][rand_send_thread_idx].lock().unwrap().send(resp_message, &MessageType::new("remote_kv_response".to_string()));
    //     {
    //         let curr_time = SystemTime::now()
    //             .duration_since(SystemTime::UNIX_EPOCH)
    //             .unwrap()
    //             .as_millis() as u64;
    //         let mut delta = 0.0;
    //         if curr_time > start_ms_since_epoch {
    //             delta = (curr_time - start_ms_since_epoch) as f64;
    //         }
    //         REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
    //             .with_label_values(&["4_kv_req_coord_handler_end"])
    //             .observe(delta);
    //     }
    // }
}
