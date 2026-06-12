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

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use eez_composer::{Classification, HeldPool, HeldTx, IngressClassifier};
use jsonrpsee::core::middleware::{Batch, Notification, RpcServiceT};
use jsonrpsee::core::server::{MethodResponse, ResponsePayload};
use jsonrpsee::types::{ErrorObject, Request};
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
    /// L1 provider for admission validation (nonce / balance against
    /// the canonical tip). `None` ⇒ validation skipped (standalone /
    /// no-L1 modes).
    l1_provider: Option<RootProvider>,
}

impl IngressLayer {
    #[must_use]
    pub fn new(held_pool: Arc<HeldPool>, classifier: Arc<IngressClassifier>) -> Self {
        // Admission validation reads sender nonce + balance from L1.
        // Prefer the canonical-tip RPC (the embedded node can lag 2-3
        // blocks); fall back to the embedded RPC; absent both, no
        // validation (dev/standalone).
        let l1_provider = std::env::var("EEZ_L1_TARGET_RPC_URL")
            .or_else(|_| std::env::var("EEZ_L1_RPC_URL"))
            .ok()
            .and_then(|u| u.parse().ok())
            .map(|u| {
                ProviderBuilder::new()
                    .disable_recommended_fillers()
                    .connect_http(u)
            });
        Self {
            held_pool,
            classifier,
            l1_provider,
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
            l1_provider: self.l1_provider.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IngressService<S> {
    inner: S,
    held_pool: Arc<HeldPool>,
    classifier: Arc<IngressClassifier>,
    l1_provider: Option<RootProvider>,
}

/// Reject an `eth_sendRawTransaction` at the door with a clear error —
/// the cross-chain equivalent of a normal node's mempool admission
/// checks. An invalid tx admitted to the held pool poisons entire
/// all-or-nothing bundles and breaks bundle-mates' nonce chains, so
/// rejection here is strictly kinder than the silent eviction later.
fn reject(id: jsonrpsee::types::Id<'_>, msg: String) -> MethodResponse {
    event!(
        name: "eez.ingress.cross_chain.rejected",
        Level::WARN,
        reason = %msg,
        "cross-chain tx rejected at ingress",
    );
    let payload = ResponsePayload::<B256>::error(ErrorObject::owned(-32000, msg, None::<()>));
    MethodResponse::response(id, payload, MAX_RESPONSE_SIZE)
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
        let l1_provider = self.l1_provider.clone();
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
            let chain_id = envelope.chain_id();

            match classifier.classify(to.as_ref(), chain_id) {
                Classification::L2Only => inner.call(req).await,
                Classification::CrossChain => {
                    // Admission validation (invariant 7: loud, at the
                    // door). A cross-chain tx rides an all-or-nothing L1
                    // bundle — one invalid tx fails the whole bundle in
                    // builder sim, evicting innocent bundle-mates and
                    // breaking their nonce chains. So validate like a
                    // mempool would (sender, nonce on-chain+held, balance)
                    // and reject precisely rather than admit a poison pill.
                    let Ok(sender) = envelope.recover_signer() else {
                        return reject(req.id, "signature recovery failed".into());
                    };
                    let nonce = envelope.nonce();
                    if let Some(provider) = l1_provider.as_ref() {
                        let on_chain = match provider.get_transaction_count(sender).await {
                            Ok(n) => n,
                            Err(e) => {
                                return reject(
                                    req.id,
                                    format!("L1 validation unavailable (nonce lookup): {e}"),
                                );
                            }
                        };
                        let held = held_pool.held_count_for(sender) as u64;
                        let expected = on_chain + held;
                        if nonce != expected {
                            return reject(
                                req.id,
                                format!(
                                    "invalid nonce {nonce} for {sender}: expected {expected}                                      (on-chain {on_chain} + {held} held)"
                                ),
                            );
                        }
                        let balance = match provider.get_balance(sender).await {
                            Ok(b) => b,
                            Err(e) => {
                                return reject(
                                    req.id,
                                    format!("L1 validation unavailable (balance lookup): {e}"),
                                );
                            }
                        };
                        let cost = U256::from(envelope.value())
                            + U256::from(envelope.gas_limit())
                                * U256::from(envelope.max_fee_per_gas());
                        if balance < cost {
                            return reject(
                                req.id,
                                format!(
                                    "insufficient L1 balance for {sender}: have {balance},                                      need {cost} (value + gas_limit * max_fee)"
                                ),
                            );
                        }
                    }

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
                        sender = %sender,
                        nonce,
                        "cross-chain tx held for next Sync slot",
                    );
                    held_pool.push(HeldTx {
                        raw_tx,
                        hash,
                        attempts: 0,
                        sender,
                        nonce,
                    });
                    let payload = ResponsePayload::<B256>::success(hash);
                    MethodResponse::response(req.id, payload, MAX_RESPONSE_SIZE)
                }
            }
        }
    }

    fn batch<'a>(&self, req: Batch<'a>) -> impl Future<Output = Self::BatchResponse> + Send + 'a {
        // Batches pass through unintercepted — `eth_sendRawTransaction`
        // is rarely batched, and matching a method inside a batch needs
        // per-call dispatch. Deferred until a workload needs it.
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
