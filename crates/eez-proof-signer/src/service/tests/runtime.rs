//! RPC limits, ingestion, deadlines, request-slot admission, and worker lifecycle.

use super::*;

#[test]
fn service_limits_reject_invalid_boundaries() {
    for (name, idle_timeout, request_timeout) in [
        (
            "zero stream idle timeout",
            Duration::ZERO,
            Duration::from_secs(1),
        ),
        (
            "zero request timeout",
            Duration::from_secs(1),
            Duration::ZERO,
        ),
        (
            "out-of-range stream idle timeout",
            Duration::MAX,
            Duration::from_secs(1),
        ),
        (
            "out-of-range request timeout",
            Duration::from_secs(1),
            Duration::MAX,
        ),
    ] {
        assert!(
            ServiceLimits::new(ServiceLimitsParams {
                max_window_blocks: nz(1),
                max_window_bytes: nz(1024),
                max_window_witness_items: nz(1024),
                max_transaction_state_checkpoints: 8,
                stream_idle_timeout: idle_timeout,
                request_timeout,
            })
            .is_err(),
            "{name} was accepted"
        );
    }

    let below_ceiling = limits_with(1, 1024, Duration::from_secs(1), Duration::from_secs(1));
    assert_eq!(below_ceiling.max_decoding_message_bytes(), 1024);
    let above_ceiling = limits_with(
        1,
        MAX_DECODING_MESSAGE_BYTES + 1,
        Duration::from_secs(1),
        Duration::from_secs(1),
    );
    assert_eq!(
        above_ceiling.max_decoding_message_bytes(),
        MAX_DECODING_MESSAGE_BYTES
    );
}

#[tokio::test]
async fn a_mismatched_header_rollup_is_rejected_without_waiting_for_eof() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;
    let (sender, receiver) = mpsc::channel(1);
    let mut chunk = header_chunk(5, 5);
    header_mut(&mut chunk).rollup_id = 2;
    sender.send(chunk).await.unwrap();

    let mut client = server.client().await;
    let status = tokio::time::timeout(
        Duration::from_secs(1),
        client.prove(ReceiverStream::new(receiver)),
    )
    .await
    .expect("a mismatched rollup must be rejected before EOF")
    .expect_err("a mismatched rollup must be rejected");

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "window rollup identity rejected");
    assert_eq!(inner.validator.stub_remaining(), 1);
    drop(sender);
}

#[tokio::test]
async fn a_checkpoint_plan_above_the_configured_limit_is_resource_exhausted() {
    let validator = Validator::stateless_for_test(Default::default(), TEST_SYSTEM_ADDRESS);
    let server = TestServer::with_limits(inner(validator), limits_with_checkpoint_limit(0)).await;

    let status = server.prove(stateless_transaction_window()).await;

    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    assert_eq!(
        status.message(),
        "window validation checkpoint quota exceeded"
    );
}

#[tokio::test]
async fn malformed_windows_never_reach_the_validator() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;

    let status = server
        .prove(vec![
            header_chunk(5, 7),
            block_chunk(5, 0x04, 0x05),
            block_chunk(7, 0x06, 0x07),
        ])
        .await;
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert_eq!(inner.validator.stub_remaining(), 1);

    let _response = server.attest(happy_window()).await;
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn a_chain_break_is_an_invalid_argument() {
    let server = TestServer::new(unused_validator()).await;
    let status = server
        .prove(vec![
            header_chunk(5, 6),
            block_chunk(5, 0x04, 0x05),
            block_chunk(6, 0xbb, 0x06),
        ])
        .await;
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("hash-chain break"), "{status:?}");
    assert!(
        status
            .message()
            .contains(&format!("{:#x}", alloy_primitives::B256::repeat_byte(0x05))),
        "{status:?}"
    );
    assert!(
        status
            .message()
            .contains(&format!("{:#x}", alloy_primitives::B256::repeat_byte(0xbb))),
        "{status:?}"
    );
}

#[tokio::test]
async fn stream_shape_violations_are_invalid_arguments() {
    let server = TestServer::new(unused_validator()).await;
    let mut missing_witness = block_chunk(5, 0x04, 0x05);
    block_mut(&mut missing_witness).witness = None;
    let mut nonempty_l1_block_hash = header_chunk(5, 5);
    header_mut(&mut nonempty_l1_block_hash)
        .post_batch
        .as_mut()
        .unwrap()
        .l1_block_hash = vec![0; 32];

    for (name, chunks) in [
        ("empty stream", vec![]),
        ("kindless first chunk", vec![ProveChunk { kind: None }]),
        ("block before header", vec![block_chunk(5, 0x04, 0x05)]),
        (
            "duplicate header",
            vec![
                header_chunk(5, 5),
                header_chunk(5, 5),
                block_chunk(5, 0x04, 0x05),
            ],
        ),
        (
            "kindless block chunk",
            vec![header_chunk(5, 5), ProveChunk { kind: None }],
        ),
        ("window without blocks", vec![header_chunk(5, 5)]),
        (
            "header without post_batch",
            vec![ProveChunk {
                kind: Some(prove_chunk::Kind::Header(ProveHeader {
                    rollup_id: 1,
                    from_block: 5,
                    to_block: 5,
                    post_batch: None,
                })),
            }],
        ),
        (
            "inverted bounds",
            vec![header_chunk(6, 5), block_chunk(5, 0x04, 0x05)],
        ),
        ("nonempty l1 block hash", vec![nonempty_l1_block_hash]),
        ("missing witness", vec![header_chunk(5, 5), missing_witness]),
    ] {
        let status = server.prove(chunks).await;
        assert_eq!(status.code(), Code::InvalidArgument, "{name}: {status:?}");
    }
}

#[tokio::test]
async fn an_over_quota_header_is_rejected_without_waiting_for_eof() {
    let inner = one_accepting_validator();
    let server = TestServer::with_limits(
        Arc::clone(&inner),
        limits_with(
            1,
            1024 * 1024,
            Duration::from_secs(5),
            Duration::from_secs(10),
        ),
    )
    .await;
    let (sender, receiver) = mpsc::channel(1);
    sender.send(header_chunk(5, 6)).await.unwrap();

    let mut client = server.client().await;
    let status = tokio::time::timeout(
        Duration::from_secs(1),
        client.prove(ReceiverStream::new(receiver)),
    )
    .await
    .expect("over-quota header must be rejected before EOF")
    .expect_err("over-quota header must be rejected");
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    assert_eq!(inner.validator.stub_remaining(), 1);
    drop(sender);
}

#[tokio::test]
async fn server_accepts_a_message_above_tonics_default() {
    const FIVE_MIB: usize = 5 * 1024 * 1024;
    let server = TestServer::with_limits(
        one_accepting_single_block_validator(),
        limits_with(
            1,
            6 * 1024 * 1024,
            Duration::from_secs(5),
            Duration::from_secs(10),
        ),
    )
    .await;
    let mut large_block = block_chunk(5, 0x04, 0x05);
    block_mut(&mut large_block).witness.as_mut().unwrap().state = vec![vec![0; FIVE_MIB]];

    let _response = server.attest(vec![header_chunk(5, 5), large_block]).await;
}

#[tokio::test]
async fn a_message_above_the_decoding_limit_is_resource_exhausted() {
    let server = TestServer::with_limits(
        unused_validator(),
        limits_with(1, 1024, Duration::from_secs(5), Duration::from_secs(10)),
    )
    .await;
    let mut oversized_block = block_chunk(5, 0x04, 0x05);
    block_mut(&mut oversized_block)
        .witness
        .as_mut()
        .unwrap()
        .state = vec![vec![0; 2 * 1024]];

    let status = server
        .prove(vec![header_chunk(5, 5), oversized_block])
        .await;
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    assert_eq!(status.message(), "Prove message exceeds decoding limit");
}

#[tokio::test]
async fn a_complete_prefix_requires_eof_and_hits_the_idle_timeout() {
    let inner = one_accepting_validator();
    let server = TestServer::with_limits(
        Arc::clone(&inner),
        limits_with(
            1,
            1024 * 1024,
            Duration::from_millis(50),
            Duration::from_secs(2),
        ),
    )
    .await;
    let (sender, receiver) = mpsc::channel(2);
    sender.send(header_chunk(5, 5)).await.unwrap();
    sender.send(block_chunk(5, 0x04, 0x05)).await.unwrap();

    let mut client = server.client().await;
    let status = tokio::time::timeout(
        Duration::from_secs(1),
        client.prove(ReceiverStream::new(receiver)),
    )
    .await
    .expect("idle timeout must terminate an open complete prefix")
    .expect_err("a stream without EOF must not validate");
    assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
    assert_eq!(inner.validator.stub_remaining(), 1);
    drop(sender);
}

#[tokio::test]
async fn the_request_deadline_is_independent_of_the_idle_timeout() {
    let inner = one_accepting_validator();
    let server = TestServer::with_limits(
        Arc::clone(&inner),
        limits_with(
            1,
            1024 * 1024,
            Duration::from_secs(5),
            Duration::from_millis(50),
        ),
    )
    .await;
    let (sender, receiver) = mpsc::channel(1);
    sender.send(header_chunk(5, 5)).await.unwrap();

    let mut client = server.client().await;
    let status = tokio::time::timeout(
        Duration::from_secs(1),
        client.prove(ReceiverStream::new(receiver)),
    )
    .await
    .expect("request deadline must terminate an open stream")
    .expect_err("a stream beyond the request deadline must not validate");
    assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
    assert_eq!(status.message(), "Prove request deadline exceeded");
    assert_eq!(inner.validator.stub_remaining(), 1);
    drop(sender);
}

#[tokio::test]
async fn a_validation_deadline_retains_the_request_slot_until_the_worker_finishes() {
    let (validator, started, release) =
        Validator::blocking_stub(Ok(backend_output_for(&happy_block_inputs())));
    let svc = ProveSvc::new(
        inner(validator),
        limits_with(
            16,
            1024 * 1024,
            Duration::from_secs(5),
            Duration::from_millis(500),
        ),
    );
    let observer = svc.clone();
    let server = TestServer::with_service(svc).await;
    let mut client = server.client().await;
    let request =
        tokio::spawn(async move { client.prove(tokio_stream::iter(happy_window())).await });

    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .expect("validation did not start")
        .expect("blocking validator dropped its start signal");
    let status = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .expect("request deadline did not return promptly")
        .expect("client task panicked")
        .expect_err("a blocked validator must exceed the request deadline");
    assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
    assert_eq!(observer.active_request_slot.available_permits(), 0);

    let saturated = server.prove(happy_window()).await;
    assert_eq!(saturated.code(), Code::ResourceExhausted, "{saturated:?}");

    release.send(()).expect("validation worker stopped early");
    tokio::time::timeout(Duration::from_secs(1), async {
        while observer.active_request_slot.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("validation completion did not release its request slot");
}

#[tokio::test]
async fn a_panicking_validation_worker_returns_a_redacted_internal_error() {
    let svc = ProveSvc::new(inner(Validator::panicking_stub()), limits());
    let observer = svc.clone();
    let server = TestServer::with_service(svc).await;

    let status = server.prove(happy_window()).await;
    assert_eq!(status.code(), Code::Internal, "{status:?}");
    assert_eq!(status.message(), "request pipeline worker failed");
    assert_eq!(observer.active_request_slot.available_permits(), 1);
}

#[test]
fn dropping_the_worker_guard_aborts_queued_work_and_signals_cancellation() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (occupier_started_tx, occupier_started_rx) = oneshot::channel();
        let (release_occupier_tx, release_occupier_rx) = std::sync::mpsc::channel();
        let occupier = tokio::task::spawn_blocking(move || {
            let _ = occupier_started_tx.send(());
            release_occupier_rx.recv().unwrap();
        });
        occupier_started_rx.await.unwrap();

        let queued_ran = Arc::new(AtomicBool::new(false));
        let queued_ran_in_worker = Arc::clone(&queued_ran);
        let queued = tokio::task::spawn_blocking(move || {
            queued_ran_in_worker.store(true, Ordering::SeqCst);
        });
        let cancellation = CancellationToken::default();
        let guard = WorkerGuard::new(queued.abort_handle(), cancellation.clone());
        assert!(!cancellation.is_cancelled());
        drop(guard);
        assert!(cancellation.is_cancelled());

        release_occupier_tx.send(()).unwrap();
        occupier.await.unwrap();
        assert!(queued.await.unwrap_err().is_cancelled());
        assert!(!queued_ran.load(Ordering::SeqCst));
    });
}

#[tokio::test]
async fn the_request_slot_is_shared_across_connections_and_released_after_cancellation() {
    let svc = ProveSvc::new(
        one_accepting_validator(),
        limits_with(
            16,
            1024 * 1024,
            Duration::from_secs(5),
            Duration::from_secs(10),
        ),
    );
    let observer = svc.clone();
    let server = TestServer::with_service(svc).await;
    let (sender, receiver) = mpsc::channel(1);
    sender.send(header_chunk(5, 5)).await.unwrap();

    let mut first_client = server.client().await;
    let first =
        tokio::spawn(async move { first_client.prove(ReceiverStream::new(receiver)).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while observer.active_request_slot.available_permits() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first request did not acquire the request slot");

    let status = server.prove(happy_window()).await;
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    drop(sender);
    tokio::time::timeout(Duration::from_secs(1), async {
        while observer.active_request_slot.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling the first client did not release its request slot");

    let _response = server.attest(happy_window()).await;
}

#[tokio::test]
async fn shutdown_waits_for_detached_request_work() {
    let svc = ProveSvc::new(unused_validator(), limits());
    let request_permit = Arc::clone(&svc.active_request_slot)
        .acquire_owned()
        .await
        .expect("the active-request semaphore is never closed");
    let observer = svc.clone();
    let wait = tokio::spawn(async move { observer.wait_until_idle().await });

    tokio::task::yield_now().await;
    assert!(!wait.is_finished());

    drop(request_permit);
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("shutdown did not observe the released request slot")
        .expect("shutdown waiter panicked");
}
