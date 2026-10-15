//! Deterministic wire boundaries, separate from scheduling/deadline stress tests.
use super::*;
use v2::client_frame::Kind as C;

fn block(data: &v1::BlockWitness) -> C {
    C::Block(v2::Block {
        data: Some(data.clone()),
    })
}

// The captured blocks are empty. Rebuild the exact DA/header projection for a
// selected range, then check its proof independently in Transcript::receive.
fn selected_finalize(template: &v2::Finalize, blocks: &[v1::BlockWitness]) -> v2::Finalize {
    let mut call = eez_protocol::abi::postAndVerifyBatchCall::abi_decode(
        &template.settlement.as_ref().unwrap().abi_calldata,
    )
    .unwrap();
    assert_eq!(call.batch.entries.len(), 1);
    let update = &mut call.batch.entries[0].rollupUpdates[0];
    update.currentRoot = B256::from_slice(&blocks[0].parent_hash);
    update.newRoot = B256::from_slice(&blocks.last().unwrap().hash);
    let span = blocks
        .iter()
        .map(|data| {
            let decoded = alloy_rlp::decode_exact::<eez_primitives::Block>(&data.rlp).unwrap();
            assert!(decoded.body.transactions.is_empty());
            eez_payload_codec::SpanBlock {
                beneficiary: decoded.header.beneficiary.0.0,
                extra_data: decoded.header.extra_data.to_vec(),
                transactions: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    call.batch.callData = eez_payload_codec::encode_container(1, &span, &[])
        .unwrap()
        .into();
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut call.batch).unwrap();
    v2::Finalize {
        from_block: blocks[0].number,
        to_block: blocks.last().unwrap().number,
        terminal_hash: blocks.last().unwrap().hash.clone(),
        settlement: Some(v2::SettlementInput {
            abi_calldata: call.abi_encode(),
            ..Default::default()
        }),
    }
}

pub(super) async fn verify(
    endpoint: &str,
    begin: &C,
    blocks: &[v1::BlockWitness],
    finalize: &v2::Finalize,
    cases: &mut Vec<Value>,
) {
    let C::Begin(anchor) = begin else {
        unreachable!()
    };

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "begin").await;
    for defect in [
        "rollup-zero",
        "wrong-rollup",
        "anchor-overflow",
        "hash-empty",
        "hash-short",
        "hash-long",
        "root-empty",
        "root-short",
        "root-long",
        "nonempty-session",
        "nonzero-epoch",
    ] {
        t.check = defect.into();
        let mut input = anchor.clone();
        let mut code = Code::InvalidArgument;
        match defect {
            "rollup-zero" => {
                input.rollup_id = 0;
                code = Code::Unimplemented;
            }
            "wrong-rollup" => {
                input.rollup_id += 1;
                code = Code::Unimplemented;
            }
            "anchor-overflow" => input.anchor_number = u64::MAX,
            "hash-empty" => input.anchor_hash.clear(),
            "hash-short" => input.anchor_hash.truncate(31),
            "hash-long" => input.anchor_hash.push(0),
            "root-empty" => input.anchor_state_root.clear(),
            "root-short" => input.anchor_state_root.truncate(31),
            "root-long" => input.anchor_state_root.push(0),
            "nonempty-session" => s.binding.session_id = vec![0xff; 32],
            "nonzero-epoch" => s.binding.epoch = 1,
            _ => unreachable!(),
        }
        t.exchange(&mut s, Some(C::Begin(input)), "rejected", Some(code))
            .await;
        s.binding = v2::ServerFrame::default();
    }
    t.check = "valid-begin-after-rejections".into();
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    t.check = "second-begin-cannot-replace-session".into();
    t.exchange(
        &mut s,
        Some(begin.clone()),
        "rejected",
        Some(Code::InvalidArgument),
    )
    .await;
    t.exchange(&mut s, Some(block(&blocks[0])), "validated", None)
        .await;
    t.close(s, true).await;
    cases.push(json!({"name": "begin-boundaries", "profile": "baseline", "steps": t.steps}));

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "block").await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    for defect in [
        "absent-data",
        "absent-witness",
        "hash-empty",
        "hash-short",
        "hash-long",
        "parent-empty",
        "parent-short",
        "parent-long",
        "number-zero",
        "number-max",
        "gap",
        "wrong-parent",
        "wrong-hash",
        "rlp-empty",
        "rlp-truncated",
        "rlp-trailing",
    ] {
        t.check = defect.into();
        let mut data = blocks[0].clone();
        let code = match defect {
            "absent-data" | "absent-witness" | "hash-empty" | "hash-short" | "hash-long"
            | "parent-empty" | "parent-short" | "parent-long" => Code::InvalidArgument,
            _ => Code::FailedPrecondition,
        };
        match defect {
            "absent-data" => {}
            "absent-witness" => data.witness = None,
            "hash-empty" => data.hash.clear(),
            "hash-short" => data.hash.truncate(31),
            "hash-long" => data.hash.push(0),
            "parent-empty" => data.parent_hash.clear(),
            "parent-short" => data.parent_hash.truncate(31),
            "parent-long" => data.parent_hash.push(0),
            "number-zero" => data.number = 0,
            "number-max" => data.number = u64::MAX,
            "gap" => data = blocks[1].clone(),
            "wrong-parent" => data.parent_hash = vec![0xff; 32],
            "wrong-hash" => data.hash = vec![0xff; 32],
            "rlp-empty" => data.rlp.clear(),
            "rlp-truncated" => {
                data.rlp.pop();
            }
            "rlp-trailing" => data.rlp.push(0),
            _ => unreachable!(),
        }
        let input = if defect == "absent-data" {
            Some(C::Block(v2::Block { data: None }))
        } else {
            Some(block(&data))
        };
        t.exchange(&mut s, input, "rejected", Some(code)).await;
    }
    for data in &blocks[..2] {
        t.exchange(&mut s, Some(block(data)), "validated", None)
            .await;
    }
    t.check = "older-duplicate-does-not-move-tip".into();
    t.exchange(&mut s, Some(block(&blocks[0])), "validated", None)
        .await;
    for defect in [
        "duplicate-rlp",
        "duplicate-parent",
        "duplicate-missing-witness",
    ] {
        t.check = defect.into();
        let mut data = blocks[0].clone();
        match defect {
            "duplicate-rlp" => data.rlp.push(0),
            "duplicate-parent" => data.parent_hash = vec![0xff; 32],
            "duplicate-missing-witness" => data.witness = None,
            _ => unreachable!(),
        }
        t.exchange(
            &mut s,
            Some(block(&data)),
            "rejected",
            Some(Code::InvalidArgument),
        )
        .await;
    }
    for data in &blocks[2..] {
        t.exchange(&mut s, Some(block(data)), "validated", None)
            .await;
    }
    t.check = "all-block-errors-recover-to-oracle-proof".into();
    t.exchange(&mut s, Some(C::Finalize(finalize.clone())), "proof", None)
        .await;
    t.close(s, false).await;
    cases.push(json!({"name": "block-boundaries", "profile": "baseline", "steps": t.steps}));

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "finalize").await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    t.check = "empty-prefix-cannot-finalize".into();
    t.exchange(
        &mut s,
        Some(C::Finalize(finalize.clone())),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    for data in blocks {
        t.exchange(&mut s, Some(block(data)), "validated", None)
            .await;
    }
    for defect in [
        "terminal-empty",
        "terminal-short",
        "terminal-long",
        "terminal-wrong",
        "terminal-before-selected-end",
        "missing-settlement",
        "empty-calldata",
        "truncated-calldata",
        "trailing-calldata",
        "from-zero",
        "from-anchor",
        "reversed-range",
        "to-unvalidated",
        "to-max",
        "from-max",
        "wrong-settlement-parent",
        "wrong-settlement-terminal",
        "wrong-proof-system",
        "wrong-rollup",
        "wrong-da-block-count",
        "bad-da",
        "sender-binding",
        "nonzero-block-number",
        "blobs",
        "posting-context",
        "blobs-and-context",
    ] {
        t.check = defect.into();
        let mut input = finalize.clone();
        let mut code = Code::FailedPrecondition;
        match defect {
            "terminal-empty" => {
                input.terminal_hash.clear();
                code = Code::InvalidArgument;
            }
            "terminal-short" => {
                input.terminal_hash.truncate(31);
                code = Code::InvalidArgument;
            }
            "terminal-long" => {
                input.terminal_hash.push(0);
                code = Code::InvalidArgument;
            }
            "terminal-wrong" => input.terminal_hash = vec![0xff; 32],
            "terminal-before-selected-end" => input.terminal_hash = blocks[0].hash.clone(),
            "missing-settlement" => {
                input.settlement = None;
                code = Code::InvalidArgument;
            }
            "empty-calldata" => {
                input.settlement.as_mut().unwrap().abi_calldata.clear();
                code = Code::InvalidArgument;
            }
            "truncated-calldata" => {
                input.settlement.as_mut().unwrap().abi_calldata.pop();
                code = Code::InvalidArgument;
            }
            "trailing-calldata" => {
                input.settlement.as_mut().unwrap().abi_calldata.push(0);
                code = Code::InvalidArgument;
            }
            "from-zero" => {
                input.from_block = 0;
                code = Code::InvalidArgument;
            }
            "from-anchor" => {
                input.from_block = anchor.anchor_number;
                code = Code::InvalidArgument;
            }
            "reversed-range" => {
                input.to_block = input.from_block - 1;
                code = Code::InvalidArgument;
            }
            "to-unvalidated" => input.to_block += 1,
            "to-max" => input.to_block = u64::MAX,
            "from-max" => {
                input.from_block = u64::MAX;
                input.to_block = u64::MAX;
            }
            "blobs" | "posting-context" | "blobs-and-context" => {
                let settlement = input.settlement.as_mut().unwrap();
                if defect != "posting-context" {
                    settlement.blobs.push(vec![0; 131072]);
                }
                if defect != "blobs" {
                    settlement.posting_context = Some(v2::L1PostingContext {
                        chain_id: 1,
                        target_timestamp: 1,
                        parent_number: 1,
                        parent_hash: vec![0x11; 32],
                    });
                }
                code = Code::Unimplemented;
            }
            _ => {
                let settlement = input.settlement.as_mut().unwrap();
                let mut call =
                    eez_protocol::abi::postAndVerifyBatchCall::abi_decode(&settlement.abi_calldata)
                        .unwrap();
                match defect {
                    "wrong-settlement-parent" => {
                        call.batch.entries[0].rollupUpdates[0].currentRoot = B256::repeat_byte(0xff)
                    }
                    "wrong-settlement-terminal" => {
                        call.batch.entries[0].rollupUpdates[0].newRoot = B256::repeat_byte(0xff)
                    }
                    "wrong-proof-system" => call.batch.proofSystems[0] = Address::repeat_byte(0xff),
                    "wrong-rollup" => call.batch.entries[0].rollupUpdates[0].rollupId += 1,
                    "wrong-da-block-count" => {
                        call.batch.callData =
                            eez_protocol::abi::postAndVerifyBatchCall::abi_decode(
                                &selected_finalize(finalize, &blocks[..1])
                                    .settlement
                                    .unwrap()
                                    .abi_calldata,
                            )
                            .unwrap()
                            .batch
                            .callData;
                    }
                    "bad-da" => {
                        call.batch.callData = vec![0xff].into();
                        code = Code::InvalidArgument;
                    }
                    "sender-binding" => call.batch.bindMsgSenderInPublicInput = true,
                    "nonzero-block-number" => call.batch.blockNumber = 1,
                    _ => unreachable!(),
                }
                eez_protocol::entries::finalize_l1_rolling_hashes(&mut call.batch).unwrap();
                settlement.abi_calldata = call.abi_encode();
            }
        }
        t.exchange(&mut s, Some(C::Finalize(input)), "rejected", Some(code))
            .await;
    }
    t.check = "finalize-errors-do-not-consume-prefix".into();
    t.exchange(&mut s, Some(C::Finalize(finalize.clone())), "proof", None)
        .await;
    t.close(s, false).await;
    cases.push(json!({"name": "finalize-boundaries", "profile": "baseline", "steps": t.steps}));

    // A valid terminal need not be the tip; optional subranges promote their
    // predecessor and must retain later blocks across Resume.
    for (name, start, end) in [
        ("finalize-one-block", 0, 1),
        ("finalize-before-tip", 0, 3),
        ("finalize-subrange", 2, 4),
        ("finalize-last-block", 5, 6),
    ] {
        let mut t = Transcript::default();
        let mut s = t.open(endpoint, "selection").await;
        t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
        for data in blocks {
            t.exchange(&mut s, Some(block(data)), "validated", None)
                .await;
        }
        t.check = name.into();
        t.exchange(
            &mut s,
            Some(C::Finalize(selected_finalize(
                finalize,
                &blocks[start..end],
            ))),
            "proof",
            None,
        )
        .await;
        let id = s.binding.session_id.clone();
        t.close(s, false).await;
        if start != 0 {
            let mut s = t.open(endpoint, "promoted").await;
            s.binding.session_id = id;
            t.check = "resume-retains-selected-range-and-later-suffix".into();
            t.exchange(&mut s, Some(C::Resume(v2::Resume {})), "ready", None)
                .await;
            t.check = "cannot-rewind-before-promoted-anchor".into();
            t.exchange(
                &mut s,
                Some(C::Rewind(v2::Rewind {
                    ancestor_hash: anchor.anchor_hash.clone(),
                })),
                "rejected",
                Some(Code::FailedPrecondition),
            )
            .await;
            t.check = "rewind-to-promoted-anchor".into();
            t.exchange(
                &mut s,
                Some(C::Rewind(v2::Rewind {
                    ancestor_hash: blocks[start - 1].hash.clone(),
                })),
                "ready",
                None,
            )
            .await;
            for data in &blocks[start..] {
                t.exchange(&mut s, Some(block(data)), "validated", None)
                    .await;
            }
            t.exchange(
                &mut s,
                Some(C::Finalize(selected_finalize(finalize, &blocks[start..]))),
                "proof",
                None,
            )
            .await;
            t.close(s, false).await;
        }
        cases.push(json!({"name": name, "profile": if start == 0 { "baseline" } else { "subrange-and-session-controls" }, "steps": t.steps}));
    }

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "resume").await;
    for length in [0, 31, 33, 32] {
        t.check = format!("resume-session-id-{length}-bytes");
        s.binding.session_id = vec![0xff; length];
        t.exchange(
            &mut s,
            Some(C::Resume(v2::Resume {})),
            "rejected",
            Some(if length == 32 {
                Code::NotFound
            } else {
                Code::InvalidArgument
            }),
        )
        .await;
    }
    s.binding = v2::ServerFrame::default();
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    t.check = "resume-on-bound-stream".into();
    t.exchange(
        &mut s,
        Some(C::Resume(v2::Resume {})),
        "rejected",
        Some(Code::InvalidArgument),
    )
    .await;
    t.exchange(&mut s, Some(block(&blocks[0])), "validated", None)
        .await;
    let mut r = t.open(endpoint, "new-attachment").await;
    r.binding.session_id = s.binding.session_id.clone();
    r.binding.epoch = 1;
    t.check = "resume-requires-epoch-zero".into();
    t.exchange(
        &mut r,
        Some(C::Resume(v2::Resume {})),
        "rejected",
        Some(Code::InvalidArgument),
    )
    .await;
    r.binding.epoch = 0;
    t.check = "resume-fences-live-attachment".into();
    t.exchange(&mut r, Some(C::Resume(v2::Resume {})), "ready", None)
        .await;
    for operation in [
        block(&blocks[1]),
        C::Finalize(finalize.clone()),
        C::Rewind(v2::Rewind {
            ancestor_hash: anchor.anchor_hash.clone(),
        }),
        C::Cancel(v2::Cancel {}),
    ] {
        t.exchange(&mut s, Some(operation), "rejected", Some(Code::Aborted))
            .await;
    }
    s.binding.epoch = r.binding.epoch;
    t.check = "old-transport-cannot-copy-new-epoch".into();
    t.exchange(
        &mut s,
        Some(block(&blocks[1])),
        "rejected",
        Some(Code::PermissionDenied),
    )
    .await;
    t.close(s, true).await;
    let id = r.binding.session_id.clone();
    t.close(r, true).await;
    let mut r = t.open(endpoint, "after-half-close").await;
    r.binding.session_id = id;
    t.check = "resume-disconnected-progress".into();
    t.exchange(&mut r, Some(C::Resume(v2::Resume {})), "ready", None)
        .await;
    for data in &blocks[1..] {
        t.exchange(&mut r, Some(block(data)), "validated", None)
            .await;
    }
    t.exchange(&mut r, Some(C::Finalize(finalize.clone())), "proof", None)
        .await;
    t.close(r, false).await;
    cases.push(
        json!({"name": "resume-boundaries", "profile": "session-controls", "steps": t.steps}),
    );

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "rewind").await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    for data in blocks {
        t.exchange(&mut s, Some(block(data)), "validated", None)
            .await;
    }
    for length in [0, 31, 33, 32] {
        t.check = format!("rewind-ancestor-{length}-bytes");
        t.exchange(
            &mut s,
            Some(C::Rewind(v2::Rewind {
                ancestor_hash: vec![0xff; length],
            })),
            "rejected",
            Some(if length == 32 {
                Code::FailedPrecondition
            } else {
                Code::InvalidArgument
            }),
        )
        .await;
    }
    t.check = "rewind-to-tip-still-fences-epoch".into();
    let old = s.binding.clone();
    t.exchange(
        &mut s,
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: blocks.last().unwrap().hash.clone(),
        })),
        "ready",
        None,
    )
    .await;
    let current = s.binding.clone();
    s.binding = old;
    t.check = "old-epoch-cannot-rewind-again".into();
    t.exchange(
        &mut s,
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: anchor.anchor_hash.clone(),
        })),
        "rejected",
        Some(Code::PermissionDenied),
    )
    .await;
    s.binding = current;
    t.check = "rewind-to-intermediate-block".into();
    t.exchange(
        &mut s,
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: blocks[1].hash.clone(),
        })),
        "ready",
        None,
    )
    .await;
    t.check = "removed-suffix-is-not-an-ancestor-even-if-cached".into();
    t.exchange(
        &mut s,
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: blocks[2].hash.clone(),
        })),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    t.check = "rewind-to-original-anchor".into();
    t.exchange(
        &mut s,
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: anchor.anchor_hash.clone(),
        })),
        "ready",
        None,
    )
    .await;
    t.check = "rewound-blocks-cannot-finalize".into();
    t.exchange(
        &mut s,
        Some(C::Finalize(finalize.clone())),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    for data in blocks {
        t.exchange(&mut s, Some(block(data)), "validated", None)
            .await;
    }
    t.exchange(&mut s, Some(C::Finalize(finalize.clone())), "proof", None)
        .await;
    t.close(s, false).await;
    cases.push(
        json!({"name": "rewind-boundaries", "profile": "session-controls", "steps": t.steps}),
    );

    let mut t = Transcript::default();
    let mut a = t.open(endpoint, "cancelled").await;
    let mut b = t.open(endpoint, "survivor").await;
    for s in [&mut a, &mut b] {
        t.exchange(s, Some(begin.clone()), "ready", None).await;
        t.exchange(s, Some(block(&blocks[0])), "validated", None)
            .await;
    }
    let binding = b.binding.clone();
    b.binding = a.binding.clone();
    t.check = "cancel-cannot-target-another-stream".into();
    t.exchange(
        &mut b,
        Some(C::Cancel(v2::Cancel {})),
        "rejected",
        Some(Code::PermissionDenied),
    )
    .await;
    b.binding = binding;
    let retired = a.binding.session_id.clone();
    t.check = "cancel-retires-only-its-session-and-closes-output".into();
    t.exchange(&mut a, Some(C::Cancel(v2::Cancel {})), "cancelled", None)
        .await;
    t.close(a, false).await;
    let mut r = t.open(endpoint, "retired").await;
    r.binding.session_id = retired;
    t.exchange(
        &mut r,
        Some(C::Resume(v2::Resume {})),
        "rejected",
        Some(Code::NotFound),
    )
    .await;
    t.close(r, true).await;
    for data in &blocks[1..] {
        t.exchange(&mut b, Some(block(data)), "validated", None)
            .await;
    }
    t.exchange(&mut b, Some(C::Finalize(finalize.clone())), "proof", None)
        .await;
    t.close(b, false).await;
    cases.push(json!({"name": "cancel-isolation", "profile": "multiple-sessions-and-controls", "steps": t.steps}));

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "framing").await;
    for operation in [
        Some(block(&blocks[0])),
        Some(C::Finalize(finalize.clone())),
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: anchor.anchor_hash.clone(),
        })),
        Some(C::Cancel(v2::Cancel {})),
    ] {
        for length in [0, 31, 33, 32] {
            t.check = format!("unbound-operation-session-{length}-bytes");
            s.binding.session_id = vec![0xff; length];
            t.exchange(
                &mut s,
                operation.clone(),
                "rejected",
                Some(if length == 32 {
                    Code::PermissionDenied
                } else {
                    Code::InvalidArgument
                }),
            )
            .await;
        }
    }
    s.binding = v2::ServerFrame::default();
    t.check = "unknown-operation-unbound".into();
    t.exchange(&mut s, None, "rejected", Some(Code::Unimplemented))
        .await;
    t.next_request = u64::MAX;
    t.check = "zero-request-id-is-correlated".into();
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    let binding = s.binding.clone();
    for defect in [
        "empty-session",
        "short-session",
        "long-session",
        "unknown-session",
        "epoch-zero",
        "epoch-max",
    ] {
        t.check = defect.into();
        match defect {
            "empty-session" => s.binding.session_id.clear(),
            "short-session" => s.binding.session_id = vec![0xff; 31],
            "long-session" => s.binding.session_id = vec![0xff; 33],
            "unknown-session" => s.binding.session_id = vec![0xff; 32],
            "epoch-zero" => s.binding.epoch = 0,
            "epoch-max" => s.binding.epoch = u64::MAX,
            _ => unreachable!(),
        }
        let code = if defect.contains("session") && defect != "unknown-session" {
            Code::InvalidArgument
        } else {
            Code::PermissionDenied
        };
        t.exchange(&mut s, Some(block(&blocks[0])), "rejected", Some(code))
            .await;
        s.binding = binding.clone();
    }
    t.check = "unknown-operation-bound".into();
    t.exchange(&mut s, None, "rejected", Some(Code::Unimplemented))
        .await;
    t.next_request = u64::MAX - 1;
    t.check = "maximum-request-id-is-correlated".into();
    t.exchange(&mut s, Some(block(&blocks[0])), "validated", None)
        .await;
    t.close(s, true).await;
    cases.push(
        json!({"name": "envelope-boundaries", "profile": "session-controls", "steps": t.steps}),
    );

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "pipeline").await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    t.check = "pipelined-gap-does-not-authorize-later-blocks".into();
    let mut pending = Vec::new();
    for (index, code) in [
        (0, None),
        (2, Some(Code::FailedPrecondition)),
        (3, Some(Code::FailedPrecondition)),
        (1, None),
    ] {
        pending.push((t.send(&s, Some(block(&blocks[index]))).await, code));
    }
    for (id, code) in pending {
        t.receive(
            &mut s,
            id,
            if code.is_some() {
                "rejected"
            } else {
                "validated"
            },
            code,
        )
        .await;
    }
    t.check = "half-close-drains-accepted-blocks-and-finalize".into();
    let mut pending = Vec::new();
    for data in &blocks[2..] {
        pending.push(t.send(&s, Some(block(data))).await);
    }
    let proof = t.send(&s, Some(C::Finalize(finalize.clone()))).await;
    t.half_close(&mut s);
    for id in pending {
        t.receive(&mut s, id, "validated", None).await;
    }
    t.receive(&mut s, proof, "proof", None).await;
    t.close(s, false).await;
    cases.push(
        json!({"name": "pipelined-gap-and-half-close", "profile": "pipelining", "steps": t.steps}),
    );

    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "execution").await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    // A new consensus hash prevents an earlier case's cache from making the
    // invalid witness test vacuous. Its valid retry must execute successfully.
    let mut unique = blocks[0].clone();
    let mut decoded = alloy_rlp::decode_exact::<eez_primitives::Block>(&unique.rlp).unwrap();
    decoded.header.extra_data = b"witness-boundary".to_vec().into();
    unique.hash = decoded.header.hash_slow().to_vec();
    unique.rlp = alloy_rlp::encode(&decoded);
    let mut bad = unique.clone();
    bad.witness = Some(v1::ExecutionWitness::default());
    t.check = "insufficient-witness-does-not-poison-hash".into();
    t.exchange(
        &mut s,
        Some(block(&bad)),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    t.exchange(&mut s, Some(block(&unique)), "validated", None)
        .await;
    t.exchange(
        &mut s,
        Some(C::Finalize(selected_finalize(finalize, &[unique]))),
        "proof",
        None,
    )
    .await;
    t.close(s, false).await;
    cases.push(json!({"name": "invalid-witness-retry", "profile": "stateless-cold-cache", "steps": t.steps}));

    let mut t = Transcript::default();
    let mut source = t.open(endpoint, "source").await;
    t.exchange(&mut source, Some(begin.clone()), "ready", None)
        .await;
    t.exchange(&mut source, Some(block(&blocks[0])), "validated", None)
        .await;
    let mut bad = t.open(endpoint, "different-parent-state").await;
    let mut wrong_anchor = anchor.clone();
    wrong_anchor.anchor_state_root = vec![0xff; 32];
    t.check = "stateless-anchor-is-a-conditional-predecessor".into();
    t.exchange(&mut bad, Some(C::Begin(wrong_anchor)), "ready", None)
        .await;
    t.check = "cached-execution-cannot-bind-another-parent-state".into();
    t.exchange(
        &mut bad,
        Some(block(&blocks[0])),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    t.close(bad, true).await;
    for data in &blocks[1..] {
        t.exchange(&mut source, Some(block(data)), "validated", None)
            .await;
    }
    t.exchange(
        &mut source,
        Some(C::Finalize(finalize.clone())),
        "proof",
        None,
    )
    .await;
    t.close(source, false).await;
    cases.push(json!({"name": "cache-parent-binding", "profile": "stateless-multiple-sessions", "steps": t.steps}));
}

pub(super) async fn verify_limits(
    begin: &C,
    blocks: &[v1::BlockWitness],
    finalize: &v2::Finalize,
    oracle: &Value,
) -> Vec<Value> {
    let bytes = blocks[..2].iter().map(Message::encoded_len).sum::<usize>();
    let items = blocks[..2]
        .iter()
        .map(|block| {
            let witness = block.witness.as_ref().unwrap();
            witness.state.len() + witness.codes.len() + witness.keys.len() + witness.headers.len()
        })
        .sum::<usize>();
    let mut cases = Vec::new();
    for (name, flag, limit) in [
        ("block-quota", "--max-request-blocks", 2),
        ("payload-quota", "--max-request-bytes", bytes),
        ("witness-quota", "--max-request-witness-items", items),
    ] {
        let args = vec![flag.to_owned(), limit.to_string()];
        let (_server, endpoint) = super::super::start_signer(oracle, &args).await;
        let mut t = Transcript::default();
        let mut s = t.open(&endpoint, "quota").await;
        t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
        t.check = "exact-quota-is-accepted".into();
        for data in &blocks[..2] {
            t.exchange(&mut s, Some(block(data)), "validated", None)
                .await;
        }
        t.check = "excess-block-rejected-without-progress".into();
        t.exchange(
            &mut s,
            Some(block(&blocks[2])),
            "rejected",
            Some(Code::ResourceExhausted),
        )
        .await;
        t.check = "duplicates-do-not-consume-quota".into();
        t.exchange(&mut s, Some(block(&blocks[0])), "validated", None)
            .await;
        let mut other = t.open(&endpoint, "independent-quota").await;
        t.exchange(&mut other, Some(begin.clone()), "ready", None)
            .await;
        t.check = "other-session-is-charged-independently-even-if-cached".into();
        for data in &blocks[..2] {
            t.exchange(&mut other, Some(block(data)), "validated", None)
                .await;
        }
        t.exchange(
            &mut other,
            Some(block(&blocks[2])),
            "rejected",
            Some(Code::ResourceExhausted),
        )
        .await;
        t.exchange(
            &mut other,
            Some(C::Cancel(v2::Cancel {})),
            "cancelled",
            None,
        )
        .await;
        t.close(other, false).await;
        t.check = "rewind-releases-only-removed-suffix".into();
        t.exchange(
            &mut s,
            Some(C::Rewind(v2::Rewind {
                ancestor_hash: blocks[0].hash.clone(),
            })),
            "ready",
            None,
        )
        .await;
        t.exchange(&mut s, Some(block(&blocks[1])), "validated", None)
            .await;
        t.exchange(
            &mut s,
            Some(block(&blocks[2])),
            "rejected",
            Some(Code::ResourceExhausted),
        )
        .await;
        t.check = "subrange-finalize-releases-only-before-promoted-anchor".into();
        t.exchange(
            &mut s,
            Some(C::Finalize(selected_finalize(finalize, &blocks[1..2]))),
            "proof",
            None,
        )
        .await;
        let id = s.binding.session_id.clone();
        t.close(s, false).await;
        let mut s = t.open(&endpoint, "after-promotion").await;
        s.binding.session_id = id;
        t.exchange(&mut s, Some(C::Resume(v2::Resume {})), "ready", None)
            .await;
        t.exchange(&mut s, Some(block(&blocks[2])), "validated", None)
            .await;
        t.exchange(
            &mut s,
            Some(block(&blocks[3])),
            "rejected",
            Some(Code::ResourceExhausted),
        )
        .await;
        t.exchange(
            &mut s,
            Some(C::Finalize(selected_finalize(finalize, &blocks[1..3]))),
            "proof",
            None,
        )
        .await;
        t.close(s, false).await;
        cases.push(json!({"name": name, "profile": "configured-limits-and-session-controls", "server_args": args, "steps": t.steps}));
    }
    // Unlike session quotas, an oversized protobuf terminates the RPC. Test
    // exactly the configured decode limit and one byte beyond it, not a huge
    // allocation depending on the machine's memory or the production limit.
    let args = vec!["--max-request-bytes".to_owned(), "4096".to_owned()];
    let (_server, endpoint) = super::super::start_signer(oracle, &args).await;
    let mut t = Transcript::default();
    let mut s = t.open(&endpoint, "message-limit").await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    t.exchange(&mut s, Some(block(&blocks[0])), "validated", None)
        .await;
    let mut padded = blocks[0].clone();
    padded.witness.as_mut().unwrap().codes.push(vec![0; 1024]);
    let frame_size = |data: &v1::BlockWitness| {
        v2::ClientFrame {
            session_id: vec![1; 32],
            request_id: 3,
            epoch: 1,
            kind: Some(block(data)),
        }
        .encoded_len()
    };
    let padding = 1024 - (frame_size(&padded) - 4096);
    padded
        .witness
        .as_mut()
        .unwrap()
        .codes
        .last_mut()
        .unwrap()
        .resize(padding, 0);
    assert_eq!(frame_size(&padded), 4096);
    t.check = "exact-message-limit-is-accepted".into();
    t.exchange(&mut s, Some(block(&padded)), "validated", None)
        .await;
    padded
        .witness
        .as_mut()
        .unwrap()
        .codes
        .last_mut()
        .unwrap()
        .push(0);
    assert_eq!(frame_size(&padded), 4097);
    t.check = "one-byte-over-limit-terminates-rpc".into();
    t.send(&s, Some(block(&padded))).await;
    t.rpc_error(&mut s, Code::ResourceExhausted).await;
    let id = s.binding.session_id.clone();
    t.close(s, false).await;
    let mut s = t.open(&endpoint, "recover-after-transport-error").await;
    s.binding.session_id = id;
    t.exchange(&mut s, Some(C::Resume(v2::Resume {})), "ready", None)
        .await;
    t.exchange(
        &mut s,
        Some(C::Finalize(selected_finalize(finalize, &blocks[..1]))),
        "proof",
        None,
    )
    .await;
    t.close(s, false).await;
    cases.push(json!({"name": "message-size-limit", "profile": "configured-limits-and-session-controls", "server_args": args, "steps": t.steps}));
    cases
}
