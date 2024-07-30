// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use crate::{RemoteKVRequest, RemoteKVResponse};
use aptos_secure_net::network_controller::{Message, MessageType, NetworkController, OutboundRpcScheduler};
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
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use aptos_drop_helper::DEFAULT_DROPPER;
use aptos_secure_net::grpc_network_service::outbound_rpc_helper::OutboundRpcHelper;
use aptos_secure_net::network_controller::metrics::REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER;


struct MpmcCpq<T> {
    messages: Mutex<ConcurrentPriorityQueue<T, u64>>,
    notify_on_sent: Notify,
}

impl<T> MpmcCpq<T> {
    pub fn send(&self, msg: T, priority: u64) {
        let mut locked_queue = self.messages.lock().unwrap();
        locked_queue.push(msg, priority);
        drop(locked_queue);

        // Send a notification to one of the calls currently
        // waiting in a call to `recv`.
        self.notify_on_sent.notify_one();
    }

    pub fn len(&self) -> usize {
        let mut locked_queue = self.messages.lock().unwrap();
        return locked_queue.len();
    }
    pub fn try_recv(&self) -> Option<T> {
        let mut locked_queue = self.messages.lock().unwrap();
        locked_queue.pop()
    }

    pub async fn recv(&self) -> T {
        let future = self.notify_on_sent.notified();
        tokio::pin!(future);
        loop {
            // Make sure that no wakeup is lost if we get
            // `None` from `try_recv`.
            future.as_mut().enable();

            if let Some(msg) = self.try_recv() {
                return msg;
            }

            // Wait for a call to `notify_one`.
            //
            // This uses `.as_mut()` to avoid consuming the future,
            // which lets us call `Pin::set` below.
            future.as_mut().await;

            // Reset the future in case another call to
            // `try_recv` got the message before us.
            future.set(self.notify_on_sent.notified());
        }
    }
}

pub struct RemoteStateViewService<S: StateView + Sync + Send + 'static> {
    kv_rx: Vec<Receiver<Message>>,
    kv_unprocessed_pq: Arc<ConcurrentPriorityQueue<Message, u64>>,
    kv_tx: Arc<Vec<Vec<Arc<tokio::sync::Mutex<OutboundRpcHelper>>>>>,
    //thread_pool: Arc<Vec<rayon::ThreadPool>>,
    //thread_pool: Arc<rayon::ThreadPool>,
    state_view: Arc<RwLock<Option<Arc<S>>>>,
    recv_condition: Arc<(tokio::sync::Mutex<bool>, Condvar)>,
    outbound_rpc_scheduler: Arc<OutboundRpcScheduler>,
    kv_proc_runtime: Arc<Runtime>,
    kv_recv_runtime: Arc<Runtime>,
    num_shards: usize,

}

impl<S: StateView + Sync + Send + 'static> RemoteStateViewService<S> {
    pub fn new(
        controller: &mut NetworkController,
        remote_shard_addresses: Vec<SocketAddr>,
        num_threads: Option<usize>,
    ) -> Self {
        let num_threads = num_threads.unwrap_or_else(num_cpus::get);
        let num_kv_req_threads = 30; //num_cpus::get() / 2;
        let num_shards = remote_shard_addresses.len();
        let num_kv_recv_threads = 16;
        info!("num threads for remote state view service: {}", num_threads);

        let kv_proc_rt = runtime::Builder::new_multi_thread()
            .worker_threads(60)
            .disable_lifo_slot()
            .enable_all()
            .thread_name("kv_proc")
            .build()
            .unwrap();

        let kv_recv_rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_kv_recv_threads)
            .disable_lifo_slot()
            .enable_all()
            .thread_name("kv_recv")
            .build()
            .unwrap();

        let kv_request_type = "remote_kv_request";
        let kv_response_type = "remote_kv_response";
        let mut result_rx = vec![];//controller.create_inbound_channel(kv_request_type.to_string());
        for shard_id in 0..num_kv_recv_threads {
            result_rx.push(controller.create_inbound_channel(format!("{}_{}", kv_request_type, shard_id)));
        }
        let command_txs = remote_shard_addresses
            .iter()
            .map(|address| {
                //controller.create_outbound_channel(*address, kv_response_type.to_string())
                let mut command_tx = vec![];
                for _ in 0..num_kv_req_threads {
                    command_tx.push(Arc::new(tokio::sync::Mutex::new(OutboundRpcHelper::new(controller.get_self_addr(), *address, controller.get_outbound_rpc_runtime()))));
                }
                command_tx
            })
            .collect_vec();
        Self {
            kv_rx: result_rx,
            kv_unprocessed_pq: Arc::new(ConcurrentPriorityQueue::new()),
            kv_tx: Arc::new(command_txs),
            //thread_pool: Arc::new(thread_pool),
            state_view: Arc::new(RwLock::new(None)),
            recv_condition: Arc::new((tokio::sync::Mutex::new(false), Condvar::new())),
            outbound_rpc_scheduler: controller.get_outbound_rpc_scheduler(),
            kv_proc_runtime: Arc::new(kv_proc_rt),
            kv_recv_runtime: Arc::new(kv_recv_rt),
            num_shards,
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

    pub fn start(&self) {
        //let (signal_tx, signal_rx) = unbounded();
        let kv_unprocessed = Arc::new(MpmcCpq {
            messages: Mutex::new(ConcurrentPriorityQueue::new()),
            notify_on_sent: Notify::new(),
        });
        let num_workers = self.kv_proc_runtime.handle().metrics().num_workers();
        let kv_proc_runtime_clone = self.kv_proc_runtime.clone();

        //info!("Num handlers created is {}", thread_pool_clone.current_num_threads());
        info!("Num handlers created is {}", num_workers);
        //for _ in 0..thread_pool_clone.current_num_threads() {
        for _ in 0..num_workers {
            let state_view_clone = self.state_view.clone();
            let kv_tx_clone = self.kv_tx.clone();
            let kv_unprocessed_clone = kv_unprocessed.clone();
            let recv_condition_clone = self.recv_condition.clone();
            let outbound_rpc_scheduler_clone = self.outbound_rpc_scheduler.clone();
            self.kv_proc_runtime.spawn(async move {
                Self::priority_handler(state_view_clone.clone(),
                                       kv_tx_clone.clone(),
                                       kv_unprocessed_clone.clone(),
                                       recv_condition_clone.clone(),
                                       outbound_rpc_scheduler_clone).await;
            });
        }
        for thread_id in 0..self.kv_rx.len() {
            let kv_unprocessed_clone = kv_unprocessed.clone();
            let kv_rx_clone = self.kv_rx[thread_id].clone();
            self.kv_recv_runtime.spawn_blocking( move || {
                while let Ok(message) = kv_rx_clone.recv() {
                    let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                    info!("kv req batch {} received from shard {} at time: {}", message.seq_num.unwrap(), message.shard_id.unwrap(), curr_time);
                    let mut delta = 0.0;
                    if curr_time > message.start_ms_since_epoch.unwrap() {
                        delta = (curr_time - message.start_ms_since_epoch.unwrap()) as f64;
                    }
                    REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                        .with_label_values(&["2_kv_req_coord_grpc_recv_spawning_req_handler"]).observe(delta);

                    let _timer = REMOTE_EXECUTOR_TIMER
                        .with_label_values(&["0", "kv_requests_handler_timer"])
                        .start_timer();

                    let priority = if message.seq_num.unwrap() == 0 {
                        0
                    } else {
                        message.seq_num.unwrap() + 2 // to give small handicap to cmd messages
                    };
                    kv_unprocessed_clone.send(message, priority);
                    REMOTE_EXECUTOR_TIMER
                        .with_label_values(&["0", "kv_req_pq_size"])
                        .observe(kv_unprocessed_clone.len() as f64);
                }
            });
        }

    }

    pub async fn priority_handler(state_view: Arc<RwLock<Option<Arc<S>>>>,
                            kv_tx: Arc<Vec<Vec<Arc<tokio::sync::Mutex<OutboundRpcHelper>>>>>,
                            pq: Arc<MpmcCpq<Message>>,
                            recv_condition: Arc<(tokio::sync::Mutex<bool>, Condvar)>,
                            outbound_rpc_scheduler: Arc<OutboundRpcScheduler>,) {
        let mut rng = StdRng::from_entropy();
        loop {
            //info!("I'm here");
            let message = pq.recv().await;
            let state_view = state_view.clone();
            let kv_txs = kv_tx.clone();

            let outbound_rpc_scheduler_clone = outbound_rpc_scheduler.clone();
            Self::handle_message(message, state_view, kv_txs, rng.gen_range(0, kv_tx[0].len()), outbound_rpc_scheduler_clone);

            // if let Some(message) = maybe_message {
            //     let state_view = state_view.clone();
            //     let kv_txs = kv_tx.clone();
            //
            //     let outbound_rpc_runtime_clone = outbound_rpc_runtime.clone();
            //     Self::handle_message(message, state_view, kv_txs, rng.gen_range(0, kv_tx[0].len()), outbound_rpc_runtime_clone).await;
            // }
        }
    }

    pub fn handle_message(
        message: Message,
        state_view: Arc<RwLock<Option<Arc<S>>>>,
        kv_tx: Arc<Vec<Vec<Arc<tokio::sync::Mutex<OutboundRpcHelper>>>>>,
        //rng: &mut StdRng,
        rand_send_thread_idx: usize,
        outbound_rpc_scheduler: Arc<OutboundRpcScheduler>,
    ) {
        let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        info!("Entered processing kv req batch {} sent to shard {} at time: {}", message.seq_num.unwrap(), message.shard_id.unwrap(), curr_time);
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
        drop(resp);
        drop(message);
        // DEFAULT_DROPPER.schedule_drop(resp);
        // DEFAULT_DROPPER.schedule_drop(message);
       // info!("Processing message with seq_num: {}", seq_num);
        let resp_message = Message::create_with_metadata(resp_serialized, start_ms_since_epoch, seq_num, shard_id as u64);
        drop(timer_3);

        let _timer_4 = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_requests_4"])
            .start_timer();

        // let timer_5 = REMOTE_EXECUTOR_TIMER
        //     .with_label_values(&["0", "kv_requests_lock"])
        //     .start_timer();
        // //let rand_send_thread_idx = rng.gen_range(0, kv_tx[shard_id].len());
        // let mtx = kv_tx[shard_id][rand_send_thread_idx].lock();
        // drop(timer_5);

        let timer_6 = REMOTE_EXECUTOR_TIMER
            .with_label_values(&["0", "kv_requests_send"])
            .start_timer();
        let kv_tx_clone = kv_tx.clone();
        outbound_rpc_scheduler.send(resp_message,
                                    MessageType::new("remote_kv_response".to_string()),
                                    kv_tx_clone[shard_id][rand_send_thread_idx].clone(),
                                    seq_num); // first 100 numbers is reserved for cmd messages
        // outbound_rpc_runtime.spawn(async move {
        //     kv_tx_clone[shard_id][rand_send_thread_idx].lock().await.send_async(resp_message, &MessageType::new("remote_kv_response".to_string())).await;
        // });
        //mtx.unwrap().send(resp_message, &MessageType::new("remote_kv_response".to_string()));

        drop(timer_6);

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
}
