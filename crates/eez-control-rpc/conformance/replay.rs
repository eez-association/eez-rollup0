//! Shared wire-vector replay for the standalone client and runtime integration tests.
use std::collections::HashMap;

use alloy_primitives::{Address, B256, Signature};
use eez_control_rpc::v2::{ClientFrame, ServerFrame, prover_client::ProverClient, server_frame};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub type Error = Box<dyn std::error::Error>;

struct Stream {
    sender: Option<mpsc::Sender<ClientFrame>>,
    responses: tonic::Streaming<ServerFrame>,
}

pub async fn replay(
    endpoint: &str,
    case: &serde_json::Value,
    attester: Address,
) -> Result<(), Error> {
    let mut streams = HashMap::<String, Stream>::new();
    let mut sessions = HashMap::<Vec<u8>, Vec<u8>>::new();
    let mut epochs = HashMap::<(Vec<u8>, u64), u64>::new();
    let mut client = ProverClient::connect(endpoint.to_owned())
        .await?
        .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
        .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES);
    for (index, step) in case["steps"]
        .as_array()
        .ok_or("missing steps")?
        .iter()
        .enumerate()
    {
        let name = step["stream"].as_str().ok_or("missing stream")?;
        let action = step["action"].as_str().ok_or("missing action")?;
        match action {
            "open" => {
                let (sender, receiver) = mpsc::channel(16);
                let responses = client
                    .prove_stream(ReceiverStream::new(receiver))
                    .await?
                    .into_inner();
                if streams
                    .insert(
                        name.to_owned(),
                        Stream {
                            sender: Some(sender),
                            responses,
                        },
                    )
                    .is_some()
                {
                    return Err("duplicate stream".into());
                }
            }
            "send" => {
                let bytes = hex::decode(step["protobuf_hex"].as_str().ok_or("missing frame")?)?;
                let mut frame = ClientFrame::decode(bytes.as_slice())?;
                if !frame.session_id.is_empty() {
                    // Unmapped IDs/epochs are deliberately malformed or unknown
                    // inputs, not placeholders. Preserve them for boundary cases.
                    frame.epoch = *epochs
                        .get(&(frame.session_id.clone(), frame.epoch))
                        .unwrap_or(&frame.epoch);
                    if let Some(id) = sessions.get(&frame.session_id) {
                        frame.session_id = id.clone();
                    }
                }
                streams
                    .get(name)
                    .ok_or("unknown stream")?
                    .sender
                    .as_ref()
                    .ok_or("closed sender")?
                    .send(frame)
                    .await?;
            }
            "receive" => {
                let bytes = hex::decode(step["protobuf_hex"].as_str().ok_or("missing frame")?)?;
                let expected = ServerFrame::decode(bytes.as_slice())?;
                let mut actual = streams
                    .get_mut(name)
                    .ok_or("unknown stream")?
                    .responses
                    .message()
                    .await?
                    .ok_or("premature EOF")?;
                if matches!(actual.kind, Some(server_frame::Kind::Ready(_))) {
                    if actual.session_id.len() != 32 || actual.epoch == 0 {
                        return Err("invalid Ready binding".into());
                    }
                    if let Some(id) = sessions.get(&expected.session_id) {
                        if id != &actual.session_id {
                            return Err("session changed during recovery".into());
                        }
                    } else {
                        if sessions.values().any(|id| id == &actual.session_id) {
                            return Err("session reused across Composers".into());
                        }
                        sessions.insert(expected.session_id.clone(), actual.session_id.clone());
                    }
                    let key = (expected.session_id.clone(), expected.epoch);
                    if let Some(epoch) = epochs.get(&key) {
                        if *epoch != actual.epoch {
                            return Err("epoch changed unexpectedly".into());
                        }
                    } else {
                        if epochs.iter().any(|((id, _), epoch)| {
                            id == &expected.session_id && *epoch == actual.epoch
                        }) {
                            return Err("control operation did not fence the old epoch".into());
                        }
                        epochs.insert(key, actual.epoch);
                    }
                }
                if !expected.session_id.is_empty() {
                    if sessions.get(&expected.session_id) != Some(&actual.session_id)
                        || epochs.get(&(expected.session_id.clone(), expected.epoch))
                            != Some(&actual.epoch)
                    {
                        return Err(format!("{name} step {index}: response crossed binding").into());
                    }
                    actual.session_id.clone_from(&expected.session_id);
                    actual.epoch = expected.epoch;
                }
                match actual.kind.as_mut() {
                    Some(server_frame::Kind::Validated(value)) => value.reused = false,
                    Some(server_frame::Kind::Rejected(value)) => value.message.clear(),
                    Some(server_frame::Kind::Proof(proof)) => {
                        let Some(server_frame::Kind::Proof(expected_proof)) = &expected.kind else {
                            return Err("unsolicited Proof".into());
                        };
                        let hash = B256::try_from(expected_proof.public_inputs_hash.as_slice())?;
                        if Signature::from_raw(&proof.proof)?.recover_address_from_prehash(&hash)?
                            != attester
                        {
                            return Err("proof does not recover the configured attester over the expected digest".into());
                        }
                        // ECDSA nonce choice is not a wire contract. Still compare
                        // the returned digest and every other frame field below.
                        proof.proof.clone_from(&expected_proof.proof);
                    }
                    _ => {}
                }
                if actual != expected {
                    return Err(format!("{name} step {index}: response differs\nexpected {expected:?}\nactual {actual:?}").into());
                }
            }
            "half_close" => {
                streams.get_mut(name).ok_or("unknown stream")?.sender = None;
            }
            "rpc_error" => {
                let code = step["code"].as_i64().ok_or("missing status code")?;
                match streams
                    .get_mut(name)
                    .ok_or("unknown stream")?
                    .responses
                    .message()
                    .await
                {
                    Err(status) if i64::from(i32::from(status.code())) == code => {}
                    other => {
                        return Err(format!(
                            "{name} step {index}: expected transport status {code}, got {other:?}"
                        )
                        .into());
                    }
                }
            }
            "eof" => {
                let mut stream = streams.remove(name).ok_or("unknown stream")?;
                // Do not half-close implicitly: terminal replies must close the RPC.
                if stream.responses.message().await?.is_some() {
                    return Err("unsolicited response after completion".into());
                }
            }
            _ => return Err(format!("unknown transcript action {action}").into()),
        }
    }
    if !streams.is_empty() {
        return Err("transcript left an open stream".into());
    }
    Ok(())
}
