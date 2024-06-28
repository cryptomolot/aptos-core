// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::format_err;
use aptos_crypto::HashValue;
use aptos_types::{
    account_address::AccountAddress,
    account_config::{self, aptos_test_root_address},
    chain_id::ChainId,
    state_store::StateView,
    transaction::{ChangeSet, Script, Version},
};
use aptos_vm::{
    data_cache::AsMoveResolver,
    move_vm_ext::{GenesisMoveVM, SessionExt},
};
use move_core_types::{
    identifier::Identifier,
    language_storage::{ModuleId, TypeTag},
    transaction_argument::convert_txn_args,
    value::{serialize_values, MoveValue},
};
use move_vm_runtime::module_traversal::{TraversalContext, TraversalStorage};
use move_vm_types::gas::UnmeteredGasMeter;

pub struct GenesisSession<'r, 'l>(SessionExt<'r, 'l>);

impl<'r, 'l> GenesisSession<'r, 'l> {
    pub fn exec_func(
        &mut self,
        module_name: &str,
        function_name: &str,
        ty_args: Vec<TypeTag>,
        args: Vec<Vec<u8>>,
    ) {
        let traversal_storage = TraversalStorage::new();
        self.0
            .execute_function_bypass_visibility(
                &ModuleId::new(
                    account_config::CORE_CODE_ADDRESS,
                    Identifier::new(module_name).unwrap(),
                ),
                &Identifier::new(function_name).unwrap(),
                ty_args,
                args,
                &mut UnmeteredGasMeter,
                &mut TraversalContext::new(&traversal_storage),
            )
            .unwrap_or_else(|e| {
                panic!(
                    "Error calling {}.{}: {}",
                    module_name,
                    function_name,
                    e.into_vm_status()
                )
            });
    }

    pub fn exec_script(&mut self, sender: AccountAddress, script: &Script) {
        let mut temp = vec![sender.to_vec()];
        temp.extend(convert_txn_args(script.args()));
        let traversal_storage = TraversalStorage::new();
        self.0
            .execute_script(
                script.code().to_vec(),
                script.ty_args().to_vec(),
                temp,
                &mut UnmeteredGasMeter,
                &mut TraversalContext::new(&traversal_storage),
            )
            .unwrap()
    }

    fn disable_reconfiguration(&mut self) {
        self.exec_func(
            "Reconfiguration",
            "disable_reconfiguration",
            vec![],
            serialize_values(&vec![MoveValue::Signer(aptos_test_root_address())]),
        )
    }

    fn enable_reconfiguration(&mut self) {
        self.exec_func(
            "Reconfiguration",
            "enable_reconfiguration",
            vec![],
            serialize_values(&vec![MoveValue::Signer(aptos_test_root_address())]),
        )
    }

    pub fn set_aptos_version(&mut self, version: Version) {
        self.exec_func(
            "AptosVersion",
            "set_version",
            vec![],
            serialize_values(&vec![
                MoveValue::Signer(aptos_test_root_address()),
                MoveValue::U64(version),
            ]),
        )
    }
}

pub fn build_changeset<S: StateView, F>(
    state_view: &S,
    procedure: F,
    chain_id: ChainId,
    genesis_id: HashValue,
) -> ChangeSet
where
    F: FnOnce(&mut GenesisSession),
{
    let vm = GenesisMoveVM::new(chain_id);

    let (change_set, module_write_set) = {
        let resolver = state_view.as_move_resolver();
        let mut session = GenesisSession(vm.new_genesis_session(&resolver, genesis_id));
        session.disable_reconfiguration();
        procedure(&mut session);
        session.enable_reconfiguration();

        // FIXME(George): more of a note: do we need this at all?
        session
            .0
            .finish(&vm.genesis_change_set_configs(), vec![])
            .map_err(|err| format_err!("Unexpected VM Error: {:?}", err))
            .unwrap()
    };

    // Genesis never produces the delta change set.
    assert!(change_set.aggregator_v1_delta_set().is_empty());
    change_set
        .try_combine_into_storage_change_set(module_write_set)
        .expect("Conversion from VMChangeSet into ChangeSet should always succeed")
}
