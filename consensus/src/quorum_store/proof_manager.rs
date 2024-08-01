// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::batch_store::BatchStore;
use crate::{
    monitor,
    quorum_store::{
        batch_generator::BackPressure,
        counters,
        utils::{BatchSortKey, ProofQueue},
    },
};
use aptos_consensus_types::{
    common::{Payload, PayloadFilter, ProofWithData, TxnSummaryWithExpiration},
    payload::{InlineBatch, OptQuorumStorePayload, PayloadExecutionLimit},
    proof_of_store::{BatchInfo, ProofOfStore, ProofOfStoreMsg},
    request_response::{GetPayloadCommand, GetPayloadResponse},
    utils::PayloadTxnsSize,
};
use aptos_logger::prelude::*;
use aptos_types::{transaction::SignedTransaction, PeerId};
use futures::StreamExt;
use futures_channel::mpsc::Receiver;
use rand::{seq::SliceRandom, thread_rng};
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

#[derive(Debug)]
pub enum ProofManagerCommand {
    ReceiveProofs(ProofOfStoreMsg),
    ReceiveBatches(Vec<(BatchInfo, Vec<TxnSummaryWithExpiration>)>),
    CommitNotification(u64, Vec<BatchInfo>),
    Shutdown(tokio::sync::oneshot::Sender<()>),
}

pub struct BatchQueue {
    batch_store: Arc<BatchStore>,
    // Queue per peer to ensure fairness between peers and priority within peer
    author_to_batches: HashMap<PeerId, BTreeMap<BatchSortKey, BatchInfo>>,
}

impl BatchQueue {
    pub fn new(batch_store: Arc<BatchStore>) -> Self {
        Self {
            batch_store,
            author_to_batches: HashMap::new(),
        }
    }

    pub fn add_batches(&mut self, batches: Vec<BatchInfo>) {
        for batch in batches.into_iter() {
            let queue = self.author_to_batches.entry(batch.author()).or_default();
            queue.insert(BatchSortKey::from_info(&batch), batch.clone());
        }
    }

    pub fn remove_batch(&mut self, batch: &BatchInfo) {
        if let Some(batch_tree) = self.author_to_batches.get_mut(&batch.author()) {
            batch_tree.remove(&BatchSortKey::from_info(batch));
        }
    }

    pub fn remove_expired_batches(&mut self) {
        let authors = self.author_to_batches.keys().cloned().collect::<Vec<_>>();
        for author in authors {
            if let Some(batch_tree) = self.author_to_batches.get_mut(&author) {
                batch_tree.retain(|_batch_key, batch| !batch.is_expired());
            }
        }
    }

    pub fn len(&self) -> usize {
        self.author_to_batches
            .values()
            .map(|batch_tree| batch_tree.len())
            .sum()
    }

    pub fn pull_batches(
        &mut self,
        max_txns: PayloadTxnsSize,
        excluded_batches: Vec<BatchInfo>,
    ) -> (Vec<(BatchInfo, Vec<SignedTransaction>)>, PayloadTxnsSize) {
        let mut result: Vec<(BatchInfo, Vec<SignedTransaction>)> = vec![];
        let mut num_txns = PayloadTxnsSize::zero();
        let mut iters = vec![];
        let mut full = false;
        for (_, batches) in self.author_to_batches.iter() {
            iters.push(batches.iter().rev());
        }
        while !iters.is_empty() {
            iters.shuffle(&mut thread_rng());
            iters.retain_mut(|iter| {
                if full {
                    return false;
                }
                if let Some((_sort_key, batch)) = iter.next() {
                    if excluded_batches.contains(batch) {
                        true
                    } else if num_txns + batch.size() <= max_txns
                    {
                        if let Ok(mut persisted_value) =
                            self.batch_store.get_batch_from_local(batch.digest())
                        {
                            if let Some(txns) = persisted_value.take_payload() {
                                num_txns += batch.size();
                                result.push((batch.clone(), txns.clone()));
                            }
                        } else {
                            warn!("Couldn't find a batch in local storage while creating inline block: {:?}", batch.digest());
                        }
                        true
                    } else {
                        full = true;
                        false
                    }
                } else {
                    false
                }
            })
        }
        (result, num_txns)
    }
}

pub struct ProofManager {
    proofs_for_consensus: ProofQueue,
    batch_queue: BatchQueue,
    back_pressure_total_txn_limit: u64,
    remaining_total_txn_num: u64,
    back_pressure_total_proof_limit: u64,
    remaining_total_proof_num: u64,
    allow_batches_without_pos_in_proposal: bool,
    enable_opt_quorum_store: bool,
}

impl ProofManager {
    pub fn new(
        my_peer_id: PeerId,
        back_pressure_total_txn_limit: u64,
        back_pressure_total_proof_limit: u64,
        batch_store: Arc<BatchStore>,
        allow_batches_without_pos_in_proposal: bool,
        enable_opt_quorum_store: bool,
    ) -> Self {
        Self {
            proofs_for_consensus: ProofQueue::new(my_peer_id),
            batch_queue: BatchQueue::new(batch_store),
            back_pressure_total_txn_limit,
            remaining_total_txn_num: 0,
            back_pressure_total_proof_limit,
            remaining_total_proof_num: 0,
            allow_batches_without_pos_in_proposal,
            enable_opt_quorum_store,
        }
    }

    pub(crate) fn receive_proofs(&mut self, proofs: Vec<ProofOfStore>) {
        for proof in proofs.into_iter() {
            self.batch_queue.remove_batch(proof.info());
            self.proofs_for_consensus.push(proof);
        }
        (self.remaining_total_txn_num, self.remaining_total_proof_num) =
            self.proofs_for_consensus.remaining_txns_and_proofs();
    }

    pub(crate) fn receive_batches(
        &mut self,
        batch_summaries: Vec<(BatchInfo, Vec<TxnSummaryWithExpiration>)>,
    ) {
        if self.allow_batches_without_pos_in_proposal {
            let batches = batch_summaries
                .iter()
                .map(|(batch_info, _)| batch_info.clone())
                .collect();
            self.batch_queue.add_batches(batches);
        }
        self.proofs_for_consensus
            .add_batch_summaries(batch_summaries);
    }

    pub(crate) fn handle_commit_notification(
        &mut self,
        block_timestamp: u64,
        batches: Vec<BatchInfo>,
    ) {
        trace!(
            "QS: got clean request from execution at block timestamp {}",
            block_timestamp
        );
        self.batch_queue.remove_expired_batches();
        for batch in &batches {
            self.batch_queue.remove_batch(batch);
        }
        self.proofs_for_consensus.mark_committed(batches);
        self.proofs_for_consensus
            .handle_updated_block_timestamp(block_timestamp);
        (self.remaining_total_txn_num, self.remaining_total_proof_num) =
            self.proofs_for_consensus.remaining_txns_and_proofs();
    }

    pub(crate) fn handle_proposal_request(&mut self, msg: GetPayloadCommand) {
        match msg {
            GetPayloadCommand::GetPayloadRequest(request) => {
                let excluded_batches: HashSet<_> = match request.filter {
                    PayloadFilter::Empty => HashSet::new(),
                    PayloadFilter::DirectMempool(_) => {
                        unreachable!()
                    },
                    PayloadFilter::InQuorumStore(proofs) => proofs,
                };

                let max_txns_with_proof = request
                    .max_txns
                    .compute_pct(100 - request.opt_batch_txns_pct);

                let (
                    proof_block,
                    txns_with_proof_size,
                    cur_unique_txns,
                    proof_queue_fully_utilized,
                ) = self.proofs_for_consensus.pull_proofs(
                    &excluded_batches,
                    max_txns_with_proof,
                    request.max_unique_txns,
                    request.return_non_full,
                    request.block_timestamp,
                );

                counters::NUM_BATCHES_WITHOUT_PROOF_OF_STORE.observe(self.batch_queue.len() as f64);
                counters::PROOF_QUEUE_FULLY_UTILIZED
                    .observe(if proof_queue_fully_utilized { 1.0 } else { 0.0 });

                let excluded_batches: Vec<_> = excluded_batches.iter().cloned().collect();

                let (opt_batches, opt_batch_txns_size) = if self.enable_opt_quorum_store {
                    // TODO(ibalajiarun): Support unique txn calculation
                    let max_opt_batch_txns_size = request.max_txns - txns_with_proof_size;
                    let (opt_batches, size) = self
                        .batch_queue
                        .pull_batches(max_opt_batch_txns_size, excluded_batches.clone());

                    (
                        opt_batches
                            .into_iter()
                            .map(|(batch_info, _)| batch_info)
                            .collect(),
                        size,
                    )
                } else {
                    (Vec::new(), PayloadTxnsSize::zero())
                };

                let cur_txns = txns_with_proof_size + opt_batch_txns_size;
                let (inline_block, inline_block_size) =
                    if self.allow_batches_without_pos_in_proposal && proof_queue_fully_utilized {
                        let mut max_inline_txns_to_pull = min(
                            request.max_txns.saturating_sub(cur_txns),
                            request.max_inline_txns,
                        );
                        max_inline_txns_to_pull.count = min(
                            max_inline_txns_to_pull.count,
                            request.max_unique_txns.saturating_sub(cur_unique_txns),
                        );
                        self.batch_queue.pull_batches(
                            max_inline_txns_to_pull,
                            excluded_batches
                                .iter()
                                .cloned()
                                .chain(proof_block.iter().map(|proof| proof.info().clone()))
                                .collect(),
                        )
                    } else {
                        (Vec::new(), PayloadTxnsSize::zero())
                    };
                counters::NUM_INLINE_BATCHES.observe(inline_block_size.count as f64);
                counters::NUM_INLINE_TXNS.observe(inline_block_size.bytes as f64);

                let response = if self.enable_opt_quorum_store {
                    let inline_batches = inline_block.into();
                    Payload::OptQuorumStore(OptQuorumStorePayload::new(
                        inline_batches,
                        opt_batches.into(),
                        proof_block.into(),
                        PayloadExecutionLimit::None,
                    ))
                } else if proof_block.is_empty() && inline_block.is_empty() {
                    Payload::empty(true, self.allow_batches_without_pos_in_proposal)
                } else {
                    trace!(
                        "QS: GetBlockRequest excluded len {}, block len {}, inline len {}",
                        excluded_batches.len(),
                        proof_block.len(),
                        inline_block.len()
                    );
                    Payload::QuorumStoreInlineHybrid(
                        inline_block,
                        ProofWithData::new(proof_block),
                        None,
                    )
                };

                let res = GetPayloadResponse::GetPayloadResponse(response);
                match request.callback.send(Ok(res)) {
                    Ok(_) => (),
                    Err(err) => debug!("BlockResponse receiver not available! error {:?}", err),
                }
            },
        }
    }

    /// return true when quorum store is back pressured
    pub(crate) fn qs_back_pressure(&self) -> BackPressure {
        if self.remaining_total_txn_num > self.back_pressure_total_txn_limit
            || self.remaining_total_proof_num > self.back_pressure_total_proof_limit
        {
            sample!(
                SampleRate::Duration(Duration::from_millis(200)),
                info!(
                    "Quorum store is back pressured with {} txns, limit: {}, proofs: {}, limit: {}",
                    self.remaining_total_txn_num,
                    self.back_pressure_total_txn_limit,
                    self.remaining_total_proof_num,
                    self.back_pressure_total_proof_limit
                );
            );
        }

        BackPressure {
            txn_count: self.remaining_total_txn_num > self.back_pressure_total_txn_limit,
            proof_count: self.remaining_total_proof_num > self.back_pressure_total_proof_limit,
        }
    }

    pub async fn start(
        mut self,
        back_pressure_tx: tokio::sync::mpsc::Sender<BackPressure>,
        mut proposal_rx: Receiver<GetPayloadCommand>,
        mut proof_rx: tokio::sync::mpsc::Receiver<ProofManagerCommand>,
    ) {
        let mut back_pressure = BackPressure {
            txn_count: false,
            proof_count: false,
        };

        loop {
            let _timer = counters::PROOF_MANAGER_MAIN_LOOP.start_timer();

            tokio::select! {
                    Some(msg) = proposal_rx.next() => monitor!("proof_manager_handle_proposal", {
                        self.handle_proposal_request(msg);

                        let updated_back_pressure = self.qs_back_pressure();
                        if updated_back_pressure != back_pressure {
                            back_pressure = updated_back_pressure;
                            if back_pressure_tx.send(back_pressure).await.is_err() {
                                debug!("Failed to send back_pressure for proposal");
                            }
                        }
                    }),
                    Some(msg) = proof_rx.recv() => {
                        monitor!("proof_manager_handle_command", {
                        match msg {
                            ProofManagerCommand::Shutdown(ack_tx) => {
                                ack_tx
                                    .send(())
                                    .expect("Failed to send shutdown ack to QuorumStore");
                                break;
                            },
                            ProofManagerCommand::ReceiveProofs(proofs) => {
                                self.receive_proofs(proofs.take());
                            },
                            ProofManagerCommand::ReceiveBatches(batches) => {
                                self.receive_batches(batches);
                            }
                            ProofManagerCommand::CommitNotification(block_timestamp, batches) => {
                                self.handle_commit_notification(
                                    block_timestamp,
                                    batches,
                                );
                            },
                        }
                        let updated_back_pressure = self.qs_back_pressure();
                        if updated_back_pressure != back_pressure {
                            back_pressure = updated_back_pressure;
                            if back_pressure_tx.send(back_pressure).await.is_err() {
                                debug!("Failed to send back_pressure for commit notification");
                            }
                        }
                    })
                }
            }
        }
    }
}
