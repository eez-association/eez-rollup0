//! jsonrpsee middleware that intercepts `eth_sendRawTransaction` and
//! routes cross-chain txs to a per-rollup
//! [`HeldPool`](eez_composer::HeldPool) — vanilla L2 txs pass through
//! to the standard reth pool path.
//!
//! The classification decision is delegated to an
//! [`IngressClassifier`](eez_composer::IngressClassifier). Empty
//! classifier (no cross-chain proxies configured) ⇒ every tx
//! passes through; the middleware is a no-op on the hot path.
//!
//! See `docs/plans/IMPLEMENTATION.md` §5.4.5.

use std::sync::Arc;

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{B256, Bytes, keccak256};
use eez_composer::{Classification, HeldPool, HeldTx, IngressClassifier};
use jsonrpsee::core::middleware::{Batch, Notification, RpcServiceT};
use jsonrpsee::core::server::{MethodResponse, ResponsePayload};
use jsonrpsee::types::Request;
use tower::Layer;
use tracing::{Level, event};

const METHOD: &str = "eth_sendRawTransaction";

/// Max JSON-RPC response body size we ever construct directly (we
/// only write a single 32-byte hash so this is plenty).
const MAX_RESPONSE_SIZE: usize = 1024;

/// Reusable layer for the ingress middleware. Cheaply [`Clone`]able
/// — both the `HeldPool` handle and the classifier are `Arc`s.
#[derive(Clone)]
pub struct IngressLayer {
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
}

impl IngressLayer {
    #[must_use]
    pub fn new(held_pool: Arc<HeldPool>, classifier: Arc<IngressClassifier>) -> Self {
        Self {
            held_pool,
            classifier,
        }
    }
}

impl<S> Layer<S> for IngressLayer {
    type Service = IngressService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        IngressService {
            inner,
            held_pool: self.held_pool.clone(),
            classifier: self.classifier.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
}

impl<S> RpcServiceT for IngressService<S>
where
    S: RpcServiceT<
            MethodResponse = MethodResponse,
            BatchResponse = MethodResponse,
            NotificationResponse = MethodResponse,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    type MethodResponse = S::MethodResponse;
    type NotificationResponse = S::NotificationResponse;
    type BatchResponse = S::BatchResponse;

    fn call<'a>(&self, req: Request<'a>) -> impl Future<Output = Self::MethodResponse> + Send + 'a {
        let inner = self.inner.clone();
        let held_pool = self.held_pool.clone();
        let classifier = self.classifier.clone();
        async move {
            // Fast path: not our method, or no cross-chain proxies
            // configured. Either way → vanilla reth handling.
            if req.method.as_ref() != METHOD || classifier.is_empty() {
                return inner.call(req).await;
            }

            // Try to extract the raw-tx hex string from the params.
            // On any decode hiccup, fall through to reth — reth's
            // own error path will produce the standard JSON-RPC
            // error for malformed input.
            let Some(params) = req.params.as_deref() else {
                return inner.call(req).await;
            };
            let Ok((raw_tx,)) = serde_json::from_str::<(Bytes,)>(params.get()) else {
                return inner.call(req).await;
            };
            let Ok(envelope) = TxEnvelope::decode_2718(&mut raw_tx.as_ref()) else {
                return inner.call(req).await;
            };
            let to = envelope.to();

            match classifier.classify(to.as_ref()) {
                Classification::L2Only => inner.call(req).await,
                Classification::CrossChain => {
                    // tx_hash = keccak256(EIP-2718 envelope bytes).
                    // For both legacy (type-0, where the envelope IS
                    // the RLP) and typed txs (where the envelope is
                    // `type ‖ rlp(body)`), keccak256 of the wire bytes
                    // gives the canonical tx hash.
                    let hash: B256 = keccak256(raw_tx.as_ref());
                    event!(
                        name: "eez.ingress.cross_chain.push",
                        Level::INFO,
                        tx_hash = %hash,
                        to = ?to,
                        "cross-chain tx held for next Sync slot",
                    );
                    held_pool.push(HeldTx { raw_tx, hash });
                    let payload = ResponsePayload::<B256>::success(hash);
                    MethodResponse::response(req.id, payload, MAX_RESPONSE_SIZE)
                }
            }
        }
    }

    fn batch<'a>(&self, req: Batch<'a>) -> impl Future<Output = Self::BatchResponse> + Send + 'a {
        // Don't try to intercept batches in S4.8 — eth_sendRawTransaction
        // is almost never batched in practice, and matching method
        // inside a batch requires per-call dispatch. Defer until a
        // workload actually needs it.
        self.inner.batch(req)
    }

    fn notification<'a>(
        &self,
        n: Notification<'a>,
    ) -> impl Future<Output = Self::NotificationResponse> + Send + 'a {
        // Notifications are fire-and-forget; no response. Passthrough.
        self.inner.notification(n)
    }
}
