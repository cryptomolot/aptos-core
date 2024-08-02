// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    pipeline::pipeline_phase::StatelessPipeline,
    state_computer::{ExecutionType, PipelineExecutionResult, SyncStateComputeResultFut},
    state_replication::StateComputer,
};
use aptos_consensus_types::pipelined_block::PipelinedBlock;
use aptos_crypto::HashValue;
use aptos_executor_types::ExecutorError;
use aptos_logger::{debug, info, warn};
use async_trait::async_trait;
use dashmap::DashMap;
use futures::TryFutureExt;
use std::sync::Arc;

pub struct PreExecutionRequest {
    pub block: PipelinedBlock,
}

pub struct PreExecutionPhase {
    execution_proxy: Arc<dyn StateComputer>,
    execution_futures: Arc<DashMap<HashValue, (SyncStateComputeResultFut, ExecutionType)>>,
}

impl PreExecutionPhase {
    pub fn new(execution_proxy: Arc<dyn StateComputer>, execution_futures: Arc<DashMap<HashValue, (SyncStateComputeResultFut, ExecutionType)>>) -> Self {
        Self {
            execution_proxy,
            execution_futures,
        }
    }
}

#[async_trait]
impl StatelessPipeline for PreExecutionPhase {
    type Request = PreExecutionRequest;
    type Response = ();

    const NAME: &'static str = "pre_execution";

    async fn process(&self, req: PreExecutionRequest) {
        let PreExecutionRequest {
            block,
        } = req;

        match self.execution_futures.entry(block.id()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {}
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                info!("[PreExecution] pre-execute block of epoch {} round {} id {}", block.epoch(), block.round(), block.id());
                let fut = self
                    .execution_proxy
                    .schedule_compute(block.block(), block.parent_id(), block.randomness().cloned())
                    .await;
                entry.insert((fut, ExecutionType::PreExecution));
            }
        }
    }
}
