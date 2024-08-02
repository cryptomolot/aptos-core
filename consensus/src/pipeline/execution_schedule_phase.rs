// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    pipeline::{
        execution_wait_phase::ExecutionWaitRequest,
        pipeline_phase::{CountedRequest, StatelessPipeline},
    },
    state_computer::{ExecutionType, PipelineExecutionResult, StateComputeResultFut, SyncStateComputeResultFut},
    state_replication::StateComputer,
};
use aptos_consensus_types::pipelined_block::PipelinedBlock;
use aptos_crypto::HashValue;
use aptos_executor_types::ExecutorError;
use aptos_logger::{debug, info};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::TryFutureExt;
use std::{
    collections::HashMap, fmt::{Debug, Display, Formatter}, pin::Pin, sync::Arc
};

/// [ This class is used when consensus.decoupled = true ]
/// ExecutionSchedulePhase is a singleton that receives ordered blocks from
/// the buffer manager and send them to the ExecutionPipeline.

pub struct ExecutionRequest {
    pub ordered_blocks: Vec<PipelinedBlock>,
    // Hold a CountedRequest to guarantee the executor doesn't get reset with pending tasks
    // stuck in the ExecutinoPipeline.
    pub lifetime_guard: CountedRequest<()>,
}

impl Debug for ExecutionRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for ExecutionRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ExecutionScheduleRequest({:?})", self.ordered_blocks)
    }
}

pub struct ExecutionSchedulePhase {
    execution_proxy: Arc<dyn StateComputer>,
    execution_futures: Arc<DashMap<HashValue, (SyncStateComputeResultFut, ExecutionType)>>,
}

impl ExecutionSchedulePhase {
    pub fn new(execution_proxy: Arc<dyn StateComputer>, execution_futures: Arc<DashMap<HashValue, (SyncStateComputeResultFut, ExecutionType)>>) -> Self {
        Self {
            execution_proxy,
            execution_futures,
        }
    }
}

#[async_trait]
impl StatelessPipeline for ExecutionSchedulePhase {
    type Request = ExecutionRequest;
    type Response = ExecutionWaitRequest;

    const NAME: &'static str = "execution_schedule";

    async fn process(&self, req: ExecutionRequest) -> ExecutionWaitRequest {
        let ExecutionRequest {
            ordered_blocks,
            lifetime_guard,
        } = req;

        let block_id = match ordered_blocks.last() {
            Some(block) => block.id(),
            None => {
                return ExecutionWaitRequest {
                    block_id: HashValue::zero(),
                    fut: Box::pin(async { Err(aptos_executor_types::ExecutorError::EmptyBlocks) }),
                }
            },
        };

        // Call schedule_compute() for each block here (not in the fut being returned) to
        // make sure they are scheduled in order.
        for block in &ordered_blocks {
            match self.execution_futures.entry(block.id()) {
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    info!("[PreExecution] block was pre-executed, epoch {} round {} id {}", block.epoch(), block.round(), block.id());
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    info!("[PreExecution] block was not pre-executed, epoch {} round {} id {}", block.epoch(), block.round(), block.id());
                    let fut = self
                        .execution_proxy
                        .schedule_compute(block.block(), block.parent_id(), block.randomness().cloned())
                        .await;
                    entry.insert((fut, ExecutionType::Execution));
                }
            }
        }

        let execution_futures = self.execution_futures.clone();

        // In the future being returned, wait for the compute results in order.
        // n.b. Must `spawn()` here to make sure lifetime_guard will be released even if
        //      ExecutionWait phase is never kicked off.
        let fut = tokio::task::spawn(async move {
            let mut results = vec![];
            for block in ordered_blocks {
                debug!("[Execution] try to receive compute result for block, epoch {} round {} id {}", block.epoch(), block.round(), block.id());
                if let Some((_, (fut, execution_type))) = execution_futures.remove(&block.id()) {
                    let PipelineExecutionResult {
                        input_txns,
                        result,
                        execution_time,
                    } = fut.await?;
                    // During pre-execution, the randomness seed is unavailable.
                    // If any transaction in the block requires randomness,
                    // it will abort with E_RANDOMNESS_SEED_UNAVAILABLE,
                    // and the block need to be re-execute through the execution pipeline.
                    if execution_type == ExecutionType::PreExecution && result.missing_randomness() {
                        debug!("[Execution] block {} of epoch {} round {} is missing randomness, need to re-execute", block.id(), block.epoch(), block.round());
                        return Err(ExecutorError::MissingRandomness);
                    }
                    results.push(block.set_execution_result(input_txns, result, execution_time));
                } else {
                    return Err(ExecutorError::internal_err(format!(
                        "Failed to find compute result for block {}",
                        block.id()
                    )));
                }
            }
            drop(lifetime_guard);
            Ok(results)
        })
        .map_err(ExecutorError::internal_err)
        .and_then(|res| async { res });

        ExecutionWaitRequest {
            block_id,
            fut: Box::pin(fut),
        }
    }
}
