//! Wire endpoint identity, message limits, and service configuration.

use super::*;

#[tokio::test]
async fn only_the_promoted_streaming_endpoint_is_served() {
    let server = TestServer::new(one_accepting_validator()).await;
    let channel = tonic::transport::Endpoint::from_shared(server.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    for path in ["/prove.v1.Prover/Prove", "/prove.v2.Prover/ProveStream"] {
        client.ready().await.unwrap();
        let result: Result<tonic::Response<tonic::Streaming<ServerFrame>>, _> = client
            .streaming(
                tonic::Request::new(tokio_stream::iter([ClientFrame::default()])),
                tonic::codegen::http::uri::PathAndQuery::from_static(path),
                tonic_prost::ProstCodec::default(),
            )
            .await;
        assert_eq!(result.unwrap_err().code(), Code::Unimplemented, "{path}");
    }
    // The same listener accepts and signs a complete prove.v1.ProveStream request.
    server.exchange(happy_window()).await.unwrap();
}

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
async fn mismatched_rollup_is_rejected_before_validation() {
    let state = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&state)).await;
    let mut window = happy_window();
    window.rollup_id = 2;
    assert_eq!(server.prove(window).await.code(), Code::Unimplemented);
    assert_eq!(state.backend.remaining_test_actions(), Some(1));
}

#[tokio::test]
async fn decoding_limits_apply_to_streaming_messages() {
    for (max_bytes, expected) in [
        (8 * 1024 * 1024, None),
        (1024, Some(Code::ResourceExhausted)),
    ] {
        let server = TestServer::with_limits(
            one_accepting_single_block_validator(),
            limits_with(1, max_bytes, Duration::from_secs(5), Duration::from_secs(5)),
        )
        .await;
        let mut block = block_chunk(5, 4, 5);
        block
            .witness
            .as_mut()
            .unwrap()
            .codes
            .push(vec![0; 5 * 1024 * 1024]);
        let result = server.exchange(TestWindow::new(5, 5, vec![block])).await;
        match expected {
            None => {
                result.unwrap();
            }
            Some(code) => assert_eq!(result.unwrap_err().code(), code),
        }
    }
}
