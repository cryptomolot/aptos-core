// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};
use crate::sharded_block_executor::{ExecutorShardCommand, StreamedExecutorShardCommand};
use aptos_types::{state_store::StateView, transaction::TransactionOutput};
use move_core_types::vm_status::VMStatus;
use crate::sharded_block_executor::sharded_executor_service::TransactionIdxAndOutput;

// Interface to communicate from the executor shards to the block executor coordinator.
pub trait CoordinatorClient<S: StateView + Sync + Send + 'static>: Send + Sync {
    fn receive_execute_command(&self) -> ExecutorShardCommand<S>;

    fn receive_execute_command_stream(&self) -> StreamedExecutorShardCommand<S>;

    fn reset_block_init(&self);

    fn reset_state_view(&self);

    fn send_execution_result(&mut self, result: Result<Vec<Vec<TransactionOutput>>, VMStatus>);

    fn stream_execution_result(&mut self, txn_idx_output: Vec<TransactionIdxAndOutput>);

    fn record_execution_complete_time_on_shard(&self);
}
