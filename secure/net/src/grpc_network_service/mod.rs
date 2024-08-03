// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod outbound_rpc_helper;

use crate::network_controller::{metrics::NETWORK_HANDLER_TIMER, Message, MessageType};
use aptos_logger::{error, info};
use aptos_protos::remote_executor::v1::{
    network_message_service_client::NetworkMessageServiceClient,
    network_message_service_server::{NetworkMessageService, NetworkMessageServiceServer},
    Empty, NetworkMessage, FILE_DESCRIPTOR_SET,
};
use crossbeam_channel::Sender;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use std::sync::RwLock;
use std::time::{Duration, SystemTime};
use tokio::{runtime::Runtime, sync::oneshot};
use tokio::time::timeout;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};
use crate::network_controller::metrics::{REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER, REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER};

const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 80;

pub struct GRPCNetworkMessageServiceServerWrapper {
    inbound_handlers: Arc<RwLock<HashMap<MessageType, Sender<Message>>>>,
    self_addr: SocketAddr,
}

impl GRPCNetworkMessageServiceServerWrapper {
    pub fn new(
        inbound_handlers: Arc<RwLock<HashMap<MessageType, Sender<Message>>>>,
        self_addr: SocketAddr,
    ) -> Self {
        Self {
            inbound_handlers,
            self_addr,
        }
    }

    // Note: The object is consumed here. That is once the server is started, we cannot/should not
    //       use the object anymore
    pub fn start(
        self,
        rt: &Runtime,
        _service: String,
        server_addr: SocketAddr,
        rpc_timeout_ms: u64,
        server_shutdown_rx: oneshot::Receiver<()>,
    ) {
        rt.spawn(async move {
            self.start_async(server_addr, rpc_timeout_ms, server_shutdown_rx)
                .await;
        });
    }

    async fn start_async(
        self,
        server_addr: SocketAddr,
        rpc_timeout_ms: u64,
        server_shutdown_rx: oneshot::Receiver<()>,
    ) {
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        info!("Starting Server async at {:?}", server_addr);
        // NOTE: (1) serve_with_shutdown() starts the server, if successful the task does not return
        //           till the server is shutdown. Hence this should be called as a separate
        //           non-blocking task. Signal handler 'server_shutdown_rx' is needed to shutdown
        //           the server
        //       (2) There is no easy way to know if/when the server has started successfully. Hence
        //           we may need to implement a healthcheck service to check if the server is up
        Server::builder()
            .timeout(std::time::Duration::from_millis(rpc_timeout_ms))
            .add_service(
                NetworkMessageServiceServer::new(self).max_decoding_message_size(MAX_MESSAGE_SIZE),
            )
            .add_service(reflection_service)
            .serve_with_shutdown(server_addr, async {
                server_shutdown_rx.await.ok();
                info!("Received signal to shutdown server at {:?}", server_addr);
            })
            .await
            .unwrap();
        info!("Server shutdown at {:?}", server_addr);
    }
}

#[tonic::async_trait]
impl NetworkMessageService for GRPCNetworkMessageServiceServerWrapper {
    async fn simple_msg_exchange(
        &self,
        request: Request<NetworkMessage>,
    ) -> Result<Response<Empty>, Status> {
        let _timer = NETWORK_HANDLER_TIMER
            .with_label_values(&[&self.self_addr.to_string(), "inbound_msgs"])
            .start_timer();
        let remote_addr = request.remote_addr();
        let network_message = request.into_inner();
        let msg = match network_message.ms_since_epoch {
            Some(ms_since_epoch) => Message::create_with_metadata(
                network_message.message,
                ms_since_epoch,
                network_message.seq_no.unwrap_or_default(),
                network_message.shard_id.unwrap_or_default(),
            ),
            None => Message::new(network_message.message),
        };

        let message_type = MessageType::new(network_message.message_type);

        if msg.start_ms_since_epoch.is_some() {
            let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
            let mut delta = 0;
            if curr_time > msg.start_ms_since_epoch.unwrap() {
                delta = (curr_time - msg.start_ms_since_epoch.unwrap());
            }
            // REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
            //     .with_label_values(&["network_message_latency"]).observe(delta as f64);

            if message_type.get_type() == "remote_kv_request" {
                REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                    .with_label_values(&["2_kv_req_coord_grpc_recv"]).observe(delta as f64);
            } else if message_type.get_type() == "remote_kv_response" {
                REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                    .with_label_values(&["6_kv_resp_shard_grpc_recv"]).observe(delta as f64);
            }
        }

        if let Some(handler) = self.inbound_handlers.read().unwrap().get(&message_type) {
            if msg.start_ms_since_epoch.is_some() {
                let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                let mut delta = 0;
                if curr_time > msg.start_ms_since_epoch.unwrap() {
                    delta = (curr_time - msg.start_ms_since_epoch.unwrap());
                }
                if message_type.get_type() == "remote_kv_request" {
                    REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                        .with_label_values(&["2_kv_req_coord_grpc_recv_got_inbound_lock"]).observe(delta as f64);
                } else if message_type.get_type() == "remote_kv_response" {
                    REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                        .with_label_values(&["6_kv_resp_shard_grpc_recv_got_inbound_lock"]).observe(delta as f64);
                } else if message_type.get_type().starts_with("execute_command_") {
                    REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                        .with_label_values(&["4_cmd_tx_msg_grpc_recv"]).observe(delta as f64);
                } else if message_type.get_type().starts_with("execute_result_") {
                    REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                        .with_label_values(&["9_results_tx_msg_grpc_recv"]).observe(delta as f64);
                }
            }
            // Send the message to the registered handler
            handler.send(msg).unwrap();
        } else {
            error!(
                "No handler registered for sender: {:?} and msg type {:?}",
                remote_addr, message_type
            );
        }
        Ok(Response::new(Empty {}))
    }
}

pub struct GRPCNetworkMessageServiceClientWrapper {
    remote_addr: String,
    remote_channel: NetworkMessageServiceClient<Channel>,
}

impl GRPCNetworkMessageServiceClientWrapper {
    pub fn new(rt: &Runtime, remote_addr: SocketAddr) -> Self {
        Self {
            remote_addr: remote_addr.to_string(),
            remote_channel: rt
                .block_on(async { Self::get_channel(format!("http://{}", remote_addr)).await }),
        }
    }

    async fn get_channel(remote_addr: String) -> NetworkMessageServiceClient<Channel> {
        info!("Trying to connect to remote server at {:?}", remote_addr);
        let conn = tonic::transport::Endpoint::new(remote_addr)
            .unwrap()
            .connect_lazy();
        NetworkMessageServiceClient::new(conn).max_decoding_message_size(MAX_MESSAGE_SIZE)
    }

    pub async fn send_message(
        &mut self,
        sender_addr: SocketAddr,
        message: Message,
        mt: &MessageType,
    ) {
        //let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        let request = tonic::Request::new(NetworkMessage {
            message: message.data.clone(),
            message_type: mt.get_type(),
            ms_since_epoch: message.start_ms_since_epoch, // Some(curr_time)
            seq_no: message.seq_num,
            shard_id: message.shard_id,
        });

        if message.start_ms_since_epoch.is_some() {
            let curr_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
            let mut delta = 0.0;
            if curr_time > message.start_ms_since_epoch.unwrap() {
                delta = (curr_time - message.start_ms_since_epoch.unwrap()) as f64;
            }
            if mt.get_type() == "remote_kv_request" {
                REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                    .with_label_values(&["1_kv_req_grpc_shard_send"]).observe(delta);
            } else if mt.get_type() == "remote_kv_response" {
                REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                    .with_label_values(&["5_kv_resp_coord_grpc_send"]).observe(delta);
            } else if mt.get_type().starts_with("execute_command_") {
                REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                    .with_label_values(&["3_cmd_tx_msg_grpc_send"]).observe(delta);
            } else if mt.get_type().starts_with("execute_result_") {
                REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                    .with_label_values(&["8_results_tx_msg_grpc_send"]).observe(delta as f64);
            }
        }
        let msg_type = mt.get_type();
        // TODO: Retry with exponential backoff on failures
        let timer1 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        match self.remote_channel.simple_msg_exchange(request).await {
            Ok(_) => {},
            Err(e) => {
                panic!(
                    "Error '{}' sending message to {} on node {:?}",
                    e, self.remote_addr, sender_addr
                );
            },
        }
        let timer2 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        let delta = (timer2 - timer1) as f64;
        if msg_type == "remote_kv_request" {
            REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                .with_label_values(&["grpc_remote_kv_request"]).observe(delta);
        } else if msg_type == "remote_kv_response" {
            REMOTE_EXECUTOR_RND_TRP_JRNY_TIMER
                .with_label_values(&["grpc_remote_kv_response"]).observe(delta);
        } else if msg_type.starts_with("execute_command_") {
            REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                .with_label_values(&["grpc_execute_command"]).observe(delta);
        } else if msg_type.starts_with("execute_result_") {
            REMOTE_EXECUTOR_CMD_RESULTS_RND_TRP_JRNY_TIMER
                .with_label_values(&["grpc_execute_result"]).observe(delta);
        }
    }
}

#[test]
fn basic_test() {
    use aptos_config::utils;
    use std::{
        net::{IpAddr, Ipv4Addr},
        thread,
    };

    let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), utils::get_available_port());
    let message_type = "test_type".to_string();
    let server_handlers: Arc<Mutex<HashMap<MessageType, Sender<Message>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let (msg_tx, msg_rx) = crossbeam_channel::unbounded();
    server_handlers
        .lock()
        .unwrap()
        .insert(MessageType::new(message_type.clone()), msg_tx);
    let server = GRPCNetworkMessageServiceServerWrapper::new(server_handlers, server_addr);

    let rt = Runtime::new().unwrap();
    let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
    server.start(
        &rt,
        "unit tester".to_string(),
        server_addr,
        1000,
        server_shutdown_rx,
    );

    let mut grpc_client = GRPCNetworkMessageServiceClientWrapper::new(&rt, server_addr);

    let client_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), utils::get_available_port());
    let test_message_content = "test1".as_bytes().to_vec();

    // wait for the server to be ready before sending messages
    // TODO: We need to implement retry on send_message failures such that we can pass this test
    //       without this sleep
    thread::sleep(std::time::Duration::from_millis(10));

    for _ in 0..2 {
        rt.block_on(async {
            grpc_client
                .send_message(
                    client_addr,
                    Message::new(test_message_content.clone()),
                    &MessageType::new(message_type.clone()),
                )
                .await;
        });
    }

    for _ in 0..2 {
        let received_msg = msg_rx.recv().unwrap();
        assert_eq!(received_msg.data, test_message_content);
    }
    server_shutdown_tx.send(()).unwrap();
}
