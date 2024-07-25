module aptos_framework::test_schedule_txn {
    use std::signer;
    use std::string;
    use std::bcs;

    use aptos_framework::aptos_account;
    use aptos_framework::object;
    use aptos_framework::timestamp;
    use aptos_framework::transaction_context::{Self, EntryFunctionPayload};
    use aptos_framework::schedule_transaction_queue;

    struct TestStruct has key {
        a: u64,
        b: u64,
    }

    entry fun foo() acquires TestStruct {
        let v = borrow_global_mut<TestStruct>(@core_resources);
        v.a = 0;
        v.b = 0;
    }

    entry fun foo_with_arg(a: u64, b: u64) acquires TestStruct {
        let v = borrow_global_mut<TestStruct>(@core_resources);
        v.a = a;
        v.b = b;
    }

    entry fun foo_with_signer_and_arg(sender: &signer, value: u64) {
        aptos_account::transfer(sender, @0x12345, value);
    }

    entry fun foo_with_new_storage(_val: u64) {
        object::create_object(@core_resources);
    }

    entry fun cancel(sender: &signer, txn_id: vector<u8>) {
        schedule_transaction_queue::cancel(sender, txn_id);
    }

    entry fun recurring(sender: &signer) {
        let addr = signer::address_of(sender);
        let timestamp = timestamp::now_seconds();
        if (!exists<TestStruct>(addr)) {
            move_to(sender, TestStruct { a: 1, b: 2 });
        };
        let txn = schedule_transaction_queue::new_transaction(
            timestamp + 1,
            100000,
            gen_payload(b"recurring", vector[]),
            addr,
        );
        schedule_transaction_queue::insert(sender, txn);
        if (timestamp % 3 == 0) {
            let txn = schedule_transaction_queue::new_transaction(
                timestamp + 2,
                1000,
                gen_payload(b"foo", vector[]),
                addr,
            );
            schedule_transaction_queue::insert(sender, txn);
        };
        if (timestamp % 3 == 1) {
            let txn = schedule_transaction_queue::new_transaction(
                timestamp + 3,
                1000,
                gen_payload(b"foo_with_arg", vector[bcs::to_bytes(&timestamp), bcs::to_bytes(&(timestamp % 1234))]),
                addr,
            );
            schedule_transaction_queue::insert(sender, txn);
        };
        if (timestamp % 3 == 2) {
            let txn = schedule_transaction_queue::new_transaction(
                timestamp + 4,
                1000,
                gen_payload(b"foo_with_signer_and_arg", vector[bcs::to_bytes(&100)]),
                addr,
            );
            schedule_transaction_queue::insert(sender, txn);
        };

        if (timestamp % 5 == 0) {
            let i = 50;
            while (i > 0) {
                let txn = schedule_transaction_queue::new_transaction(
                    timestamp + 5 + i % 2,
                    1000,
                    gen_payload(b"foo_with_new_storage", vector[bcs::to_bytes(&i)]),
                    addr,
                );
                let id = schedule_transaction_queue::insert(sender, txn);
                if (i % 3 == 0) {
                    // schedule a closer txn to test cancel
                    let txn = schedule_transaction_queue::new_transaction(
                        timestamp + i,
                        1000,
                        gen_payload(b"cancel", vector[bcs::to_bytes(&id)]),
                        addr,
                    );
                    schedule_transaction_queue::insert(sender, txn);
                };
                i = i - 1;
            }
        };

        if (timestamp % 5 == 1) {
            // out of gas
            let txn = schedule_transaction_queue::new_transaction(
                timestamp + 4,
                0,
                gen_payload(b"foo_with_signer_and_arg", vector[bcs::to_bytes(&100)]),
                addr,
            );
            schedule_transaction_queue::insert(sender, txn);
        }
    }

    fun gen_payload(name: vector<u8>, args: vector<vector<u8>>): EntryFunctionPayload {
        transaction_context::new_entry_function_payload(@0x1, string::utf8(b"test_schedule_txn"), string::utf8(name), vector[], args)
    }
}
