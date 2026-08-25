use std::{collections::BTreeMap, time::Duration};

use alloy_primitives::Address;
use futures_util::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
    gossipsub::{self, IdentTopic, MessageAcceptance, MessageAuthenticity, ValidationMode},
    identity, noise,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{Level, event};

use crate::{authenticate_message, blocks_topic};

const COMMAND_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 128;
const REDIAL_INTERVAL: Duration = Duration::from_secs(5);
// GossipSub defaults mirrored from op-node's canonical implementation:
// https://github.com/ethereum-optimism/optimism/blob/d41f9e6af629df5a6666366b9f0dbf26184c2984/op-node/p2p/gossip.go
const GOSSIP_HEARTBEAT: Duration = Duration::from_millis(500);
const SEEN_MESSAGES_TTL: Duration = Duration::from_secs(65);
const DEFAULT_MESH_D: usize = 8;
const DEFAULT_MESH_D_LOW: usize = 6;
const DEFAULT_MESH_D_HIGH: usize = 12;
const DEFAULT_MESH_D_LAZY: usize = 6;
/// Number of recent signed payloads retained for follower gap recovery.
pub const BACKFILL_CACHE_SIZE: usize = 256;

/// OP-compatible maximum uncompressed GossipSub block size.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Typed configuration for one chain-scoped unsafe-block network.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Rollup chain ID, used in the GossipSub topic.
    pub chain_id: u64,
    /// Consensus identity authorized to sign unsafe blocks.
    pub authorized_signer: Address,
    /// Local TCP listen address, for example `/ip4/0.0.0.0/tcp/9300`.
    pub listen_addr: Multiaddr,
    /// Static peers to dial. A `/p2p/<peer-id>` suffix is optional.
    pub peers: Vec<Multiaddr>,
}

impl NetworkConfig {
    /// Parse CLI/environment multiaddrs into a typed configuration.
    pub fn parse<'a>(
        chain_id: u64,
        authorized_signer: Address,
        listen_addr: &str,
        peers: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, NetworkError> {
        let listen_addr = listen_addr
            .parse()
            .map_err(|error| NetworkError::InvalidAddress {
                address: listen_addr.to_owned(),
                error: format!("{error:?}"),
            })?;
        let peers = peers
            .into_iter()
            .map(|address| {
                address
                    .parse()
                    .map_err(|error| NetworkError::InvalidAddress {
                        address: address.to_owned(),
                        error: format!("{error:?}"),
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            chain_id,
            authorized_signer,
            listen_addr,
            peers,
        })
    }
}

/// Observable network events. The node consumes [`Message`](Self::Message);
/// listening and subscription events are also useful to tests and operators.
#[derive(Debug)]
pub enum NetworkEvent {
    /// The swarm is accepting connections at this address.
    Listening(Multiaddr),
    /// A connected peer subscribed to this chain's block topic.
    PeerSubscribed(PeerId),
    /// A Snappy-decoded signed unsafe-block message.
    Message(Vec<u8>),
    /// No connected publisher had the requested historical payload.
    BackfillUnavailable(u64),
}

/// P2P setup and runtime-boundary failures.
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    /// A configured listen or peer multiaddr is malformed.
    #[error("invalid P2P multiaddr {address:?}: {error}")]
    InvalidAddress { address: String, error: String },
    /// GossipSub configuration was invalid.
    #[error("invalid GossipSub configuration: {0}")]
    GossipConfig(String),
    /// Transport setup failed.
    #[error("could not build P2P transport: {0}")]
    Transport(String),
    /// The listen address was rejected.
    #[error("could not listen on {address}: {error}")]
    Listen { address: Multiaddr, error: String },
    /// Topic subscription failed.
    #[error("could not subscribe to {topic}: {error}")]
    Subscribe { topic: String, error: String },
    /// The network service has stopped.
    #[error("unsafe-block P2P service has stopped")]
    ServiceStopped,
}

enum NetworkCommand {
    Publish { block_number: u64, message: Vec<u8> },
    RequestPayload(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PayloadRequest {
    block_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PayloadResponse {
    block_number: u64,
    /// Snappy-compressed signed message, or `None` when absent from cache.
    message: Option<Vec<u8>>,
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "BehaviourEvent")]
struct Behaviour {
    gossip: gossipsub::Behaviour,
    backfill: request_response::cbor::Behaviour<PayloadRequest, PayloadResponse>,
    ping: libp2p::ping::Behaviour,
    identify: libp2p::identify::Behaviour,
}

#[derive(Debug)]
enum BehaviourEvent {
    Gossip(gossipsub::Event),
    Backfill(request_response::Event<PayloadRequest, PayloadResponse>),
    Ping(libp2p::ping::Event),
    Identify(Box<libp2p::identify::Event>),
}

impl From<gossipsub::Event> for BehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossip(event)
    }
}

impl From<request_response::Event<PayloadRequest, PayloadResponse>> for BehaviourEvent {
    fn from(event: request_response::Event<PayloadRequest, PayloadResponse>) -> Self {
        Self::Backfill(event)
    }
}

impl From<libp2p::ping::Event> for BehaviourEvent {
    fn from(event: libp2p::ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<libp2p::identify::Event> for BehaviourEvent {
    fn from(event: libp2p::identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

/// Clone-cheap command handle for a running network service.
#[derive(Debug, Clone)]
pub struct NetworkHandle {
    commands: mpsc::Sender<NetworkCommand>,
}

impl NetworkHandle {
    /// Queue one signed, uncompressed unsafe-block message for Snappy + GossipSub publication.
    pub async fn publish(&self, block_number: u64, message: Vec<u8>) -> Result<(), NetworkError> {
        self.commands
            .send(NetworkCommand::Publish {
                block_number,
                message,
            })
            .await
            .map_err(|_| NetworkError::ServiceStopped)
    }

    /// Request one missing payload by block number from a connected peer.
    pub async fn request_payload(&self, block_number: u64) -> Result<(), NetworkError> {
        self.commands
            .send(NetworkCommand::RequestPayload(block_number))
            .await
            .map_err(|_| NetworkError::ServiceStopped)
    }
}

/// Single-owner libp2p event loop.
pub struct NetworkService {
    swarm: Swarm<Behaviour>,
    topic: IdentTopic,
    peers: Vec<Multiaddr>,
    commands: mpsc::Receiver<NetworkCommand>,
    events: mpsc::Sender<NetworkEvent>,
    subscribed_peers: Vec<PeerId>,
    payload_cache: BTreeMap<u64, Vec<u8>>,
    authorized_signer: Address,
    chain_id: u64,
}

impl std::fmt::Debug for NetworkService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkService")
            .field("local_peer_id", self.swarm.local_peer_id())
            .field("topic", &self.topic)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

impl NetworkService {
    /// Build a TCP/Noise/Yamux GossipSub service with an ephemeral libp2p identity.
    ///
    /// The sequencer authorization key is intentionally separate and signs the
    /// protocol payload itself, so transport identity rotation cannot authorize
    /// unsafe blocks.
    pub fn new(
        config: NetworkConfig,
    ) -> Result<(Self, NetworkHandle, mpsc::Receiver<NetworkEvent>), NetworkError> {
        // OP Stack uses a secp256k1 network identity independently of the
        // application-layer sequencer key.
        let identity = identity::Keypair::generate_secp256k1();
        let gossip_config = gossipsub::ConfigBuilder::default()
            .mesh_n(DEFAULT_MESH_D)
            .mesh_n_low(DEFAULT_MESH_D_LOW)
            .mesh_n_high(DEFAULT_MESH_D_HIGH)
            .gossip_lazy(DEFAULT_MESH_D_LAZY)
            .heartbeat_interval(GOSSIP_HEARTBEAT)
            .fanout_ttl(Duration::from_secs(24))
            .history_length(12)
            .history_gossip(3)
            .flood_publish(false)
            .support_floodsub()
            .max_transmit_size(MAX_MESSAGE_SIZE)
            .duplicate_cache_time(SEEN_MESSAGES_TTL)
            // Messages have no libp2p author/signature. Noise authenticates
            // the peer connection; the sequencer signature authenticates the
            // consensus payload.
            .validation_mode(ValidationMode::None)
            .validate_messages()
            .message_id_fn(compute_message_id)
            .build()
            .map_err(|error| NetworkError::GossipConfig(error.to_string()))?;
        let gossip = gossipsub::Behaviour::new(MessageAuthenticity::Anonymous, gossip_config)
            .map_err(|error| NetworkError::GossipConfig(error.to_string()))?;
        let backfill_protocol =
            StreamProtocol::try_from_owned(format!("/eez/{}/payload_by_number/1", config.chain_id))
                .map_err(|error| NetworkError::GossipConfig(error.to_string()))?;
        let backfill_codec =
            request_response::cbor::codec::Codec::<PayloadRequest, PayloadResponse>::default()
                .set_response_size_maximum((MAX_MESSAGE_SIZE + 1024) as u64);
        let backfill = request_response::Behaviour::with_codec(
            backfill_codec,
            [(backfill_protocol, ProtocolSupport::Full)],
            request_response::Config::default(),
        );
        let ping = libp2p::ping::Behaviour::default();
        let identify = libp2p::identify::Behaviour::new(libp2p::identify::Config::new(
            "/eez/1.0.0".to_owned(),
            identity.public(),
        ));
        let behaviour = Behaviour {
            gossip,
            backfill,
            ping,
            identify,
        };
        let mut swarm = SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|error| NetworkError::Transport(error.to_string()))?
            .with_behaviour(|_| behaviour)
            .map_err(|error| NetworkError::Transport(error.to_string()))?
            .build();

        let topic_name = blocks_topic(config.chain_id);
        let topic = IdentTopic::new(topic_name.clone());
        swarm
            .behaviour_mut()
            .gossip
            .subscribe(&topic)
            .map_err(|error| NetworkError::Subscribe {
                topic: topic_name,
                error: format!("{error:?}"),
            })?;
        swarm
            .listen_on(config.listen_addr.clone())
            .map_err(|error| NetworkError::Listen {
                address: config.listen_addr,
                error: format!("{error:?}"),
            })?;

        let (command_tx, commands) = mpsc::channel(COMMAND_BUFFER);
        let (events_tx, events) = mpsc::channel(EVENT_BUFFER);
        Ok((
            Self {
                swarm,
                topic,
                peers: config.peers,
                commands,
                events: events_tx,
                subscribed_peers: Vec::new(),
                payload_cache: BTreeMap::new(),
                authorized_signer: config.authorized_signer,
                chain_id: config.chain_id,
            },
            NetworkHandle {
                commands: command_tx,
            },
            events,
        ))
    }

    /// Run until all command/event handles are dropped or the swarm terminates.
    pub async fn run(mut self) {
        let mut redial = tokio::time::interval(REDIAL_INTERVAL);
        loop {
            tokio::select! {
                _ = redial.tick() => self.dial_static_peers(),
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        return;
                    };
                    match command {
                        NetworkCommand::Publish { block_number, message } => {
                            self.publish(block_number, message);
                        }
                        NetworkCommand::RequestPayload(block_number) => {
                            self.request_payload(block_number);
                        }
                    }
                }
                swarm_event = self.swarm.select_next_some() => {
                    self.on_swarm_event(swarm_event).await;
                }
            }
        }
    }

    fn dial_static_peers(&mut self) {
        for address in &self.peers {
            if let Err(error) = self.swarm.dial(address.clone()) {
                event!(
                    name: "eez.p2p.peer.dial_failed",
                    Level::DEBUG,
                    %address,
                    %error,
                    "static unsafe-block peer dial failed",
                );
            }
        }
    }

    fn publish(&mut self, block_number: u64, message: Vec<u8>) {
        if let Err(error) = authenticate_message(&message, self.chain_id, self.authorized_signer) {
            event!(
                name: "eez.p2p.block.publish_rejected",
                Level::ERROR,
                %error,
                "refusing to publish an unauthenticated unsafe block",
            );
            return;
        }
        let compressed = match compress(&message) {
            Ok(compressed) => compressed,
            Err(error) => {
                event!(
                    name: "eez.p2p.block.publish_rejected",
                    Level::ERROR,
                    %error,
                    "signed unsafe block exceeded the transport boundary",
                );
                return;
            }
        };
        self.payload_cache.insert(block_number, compressed.clone());
        while self.payload_cache.len() > BACKFILL_CACHE_SIZE {
            self.payload_cache.pop_first();
        }
        match self
            .swarm
            .behaviour_mut()
            .gossip
            .publish(self.topic.clone(), compressed)
        {
            Ok(message_id) => event!(
                name: "eez.p2p.block.published",
                Level::DEBUG,
                %message_id,
                "published signed unsafe block",
            ),
            Err(error) => event!(
                name: "eez.p2p.block.publish_failed",
                Level::WARN,
                %error,
                "could not publish signed unsafe block",
            ),
        }
    }

    fn request_payload(&mut self, block_number: u64) {
        if self.subscribed_peers.is_empty() {
            event!(
                name: "eez.p2p.backfill.no_peer",
                Level::DEBUG,
                block.number = block_number,
                "cannot request unsafe payload without a subscribed peer",
            );
            return;
        }
        for peer_id in self.subscribed_peers.clone() {
            self.swarm
                .behaviour_mut()
                .backfill
                .send_request(&peer_id, PayloadRequest { block_number });
        }
    }

    async fn on_swarm_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                event!(
                    name: "eez.p2p.listening",
                    Level::INFO,
                    %address,
                    peer_id = %self.swarm.local_peer_id(),
                    "unsafe-block P2P listener started",
                );
                let _ = self.events.send(NetworkEvent::Listening(address)).await;
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossip(gossipsub::Event::Subscribed {
                peer_id,
                topic,
            })) if topic == self.topic.hash() => {
                if !self.subscribed_peers.contains(&peer_id) {
                    self.subscribed_peers.push(peer_id);
                }
                let _ = self
                    .events
                    .send(NetworkEvent::PeerSubscribed(peer_id))
                    .await;
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossip(gossipsub::Event::Unsubscribed {
                peer_id,
                topic,
            })) if topic == self.topic.hash() => {
                self.subscribed_peers.retain(|peer| *peer != peer_id);
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossip(gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            })) => {
                let decoded = match decompress(&message.data).and_then(|decoded| {
                    authenticate_message(&decoded, self.chain_id, self.authorized_signer)
                        .map_err(|error| error.to_string())?;
                    Ok(decoded)
                }) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        event!(
                            name: "eez.p2p.block.validation_rejected",
                            Level::DEBUG,
                            %error,
                            "rejected unauthenticated unsafe-block gossip",
                        );
                        self.swarm
                            .behaviour_mut()
                            .gossip
                            .report_message_validation_result(
                                &message_id,
                                &propagation_source,
                                MessageAcceptance::Reject,
                            );
                        return;
                    }
                };
                self.swarm
                    .behaviour_mut()
                    .gossip
                    .report_message_validation_result(
                        &message_id,
                        &propagation_source,
                        MessageAcceptance::Accept,
                    );
                let _ = self.events.send(NetworkEvent::Message(decoded)).await;
            }
            SwarmEvent::Behaviour(BehaviourEvent::Backfill(event)) => {
                self.on_backfill_event(event).await;
            }
            SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => event!(
                name: "eez.p2p.peer.ping",
                Level::TRACE,
                ?event,
                "unsafe-block peer ping event",
            ),
            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => event!(
                name: "eez.p2p.peer.identified",
                Level::TRACE,
                ?event,
                "unsafe-block peer identify event",
            ),
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                self.subscribed_peers.retain(|peer| *peer != peer_id);
            }
            _ => {}
        }
    }

    async fn on_backfill_event(
        &mut self,
        event: request_response::Event<PayloadRequest, PayloadResponse>,
    ) {
        let request_response::Event::Message { message, .. } = event else {
            return;
        };
        match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let response = PayloadResponse {
                    block_number: request.block_number,
                    message: self.payload_cache.get(&request.block_number).cloned(),
                };
                if self
                    .swarm
                    .behaviour_mut()
                    .backfill
                    .send_response(channel, response)
                    .is_err()
                {
                    event!(
                        name: "eez.p2p.backfill.response_failed",
                        Level::DEBUG,
                        block.number = request.block_number,
                        "backfill response channel closed",
                    );
                }
            }
            request_response::Message::Response { response, .. } => {
                let Some(message) = response.message else {
                    let _ = self
                        .events
                        .send(NetworkEvent::BackfillUnavailable(response.block_number))
                        .await;
                    return;
                };
                match decompress(&message).and_then(|decoded| {
                    authenticate_message(&decoded, self.chain_id, self.authorized_signer)
                        .map_err(|error| error.to_string())?;
                    Ok(decoded)
                }) {
                    Ok(decoded) => {
                        let _ = self.events.send(NetworkEvent::Message(decoded)).await;
                    }
                    Err(error) => event!(
                        name: "eez.p2p.backfill.decode_rejected",
                        Level::WARN,
                        block.number = response.block_number,
                        %error,
                        "rejected malformed Snappy backfill payload",
                    ),
                }
            }
        }
    }
}

/// OP Stack content-based message ID: SHA-256 over a Snappy validity
/// domain, the length-prefixed topic, and the decompressed payload. The
/// 20-byte truncation matches op-node and prevents alternate valid Snappy
/// encodings from bypassing GossipSub deduplication.
///
/// The Rust layout and golden vectors are cross-checked against Kona:
/// <https://github.com/ethereum-optimism/optimism/blob/d41f9e6af629df5a6666366b9f0dbf26184c2984/rust/kona/crates/node/gossip/src/config.rs>
fn compute_message_id(message: &gossipsub::Message) -> gossipsub::MessageId {
    const INVALID_SNAPPY_DOMAIN: [u8; 4] = [0, 0, 0, 0];
    const VALID_SNAPPY_DOMAIN: [u8; 4] = [1, 0, 0, 0];

    let decompressed = decompress(&message.data).ok();
    let (domain, payload) = decompressed.as_deref().map_or_else(
        || (INVALID_SNAPPY_DOMAIN, message.data.as_slice()),
        |payload| (VALID_SNAPPY_DOMAIN, payload),
    );
    let topic = message.topic.as_str().as_bytes();
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((topic.len() as u64).to_le_bytes());
    hasher.update(topic);
    hasher.update(payload);
    gossipsub::MessageId::from(hasher.finalize()[..20].to_vec())
}

fn compress(message: &[u8]) -> Result<Vec<u8>, String> {
    if message.len() > MAX_MESSAGE_SIZE {
        return Err(format!(
            "message is {} bytes, maximum is {MAX_MESSAGE_SIZE}",
            message.len()
        ));
    }
    let compressed = snap::raw::Encoder::new()
        .compress_vec(message)
        .map_err(|error| error.to_string())?;
    if compressed.len() > MAX_MESSAGE_SIZE {
        return Err(format!(
            "compressed message is {} bytes, maximum is {MAX_MESSAGE_SIZE}",
            compressed.len()
        ));
    }
    Ok(compressed)
}

fn decompress(message: &[u8]) -> Result<Vec<u8>, String> {
    if message.len() > MAX_MESSAGE_SIZE {
        return Err(format!(
            "compressed message is {} bytes, maximum is {MAX_MESSAGE_SIZE}",
            message.len()
        ));
    }
    let decompressed_len = snap::raw::decompress_len(message).map_err(|error| error.to_string())?;
    if decompressed_len > MAX_MESSAGE_SIZE {
        return Err(format!(
            "decompressed message is {decompressed_len} bytes, maximum is {MAX_MESSAGE_SIZE}"
        ));
    }
    snap::raw::Decoder::new()
        .decompress_vec(message)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, hex};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use tokio::time::{Instant, timeout};

    fn test_signer() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap()
    }

    fn local_config(port: u16, peers: Vec<Multiaddr>) -> NetworkConfig {
        NetworkConfig {
            chain_id: 1234,
            authorized_signer: test_signer().address(),
            listen_addr: format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap(),
            peers,
        }
    }

    fn signed_message(body: &[u8]) -> Vec<u8> {
        let signature = test_signer()
            .sign_hash_sync(&crate::signing_hash(1234, body))
            .unwrap();
        [signature.as_rsy().as_slice(), body].concat()
    }

    #[test]
    fn snappy_roundtrip_and_size_limit() {
        let message = vec![0x42; 32_000];
        assert_eq!(decompress(&compress(&message).unwrap()).unwrap(), message);
        assert!(compress(&vec![0; MAX_MESSAGE_SIZE + 1]).is_err());
    }

    #[test]
    fn message_id_matches_op_node_golden_vectors() {
        let make = |data: Vec<u8>| gossipsub::Message {
            source: None,
            data,
            sequence_number: None,
            topic: gossipsub::TopicHash::from_raw("test"),
        };
        assert_eq!(
            compute_message_id(&make(vec![1, 2, 3, 4, 5])).0,
            hex!("b6897dcba59347fedcd694cc0f5117093c9dc727").to_vec(),
        );
        let valid = snap::raw::Encoder::new()
            .compress_vec(&[1, 2, 3, 4, 5])
            .unwrap();
        assert_eq!(
            compute_message_id(&make(valid)).0,
            hex!("adbe547b27f41294a08a09210a5e8531e83cdc16").to_vec(),
        );
    }

    #[tokio::test]
    async fn static_peers_exchange_live_messages() {
        let (publisher, publish_handle, mut publisher_events) =
            NetworkService::new(local_config(0, Vec::new())).unwrap();
        let publisher_task = tokio::spawn(publisher.run());
        let publish_addr = match timeout(Duration::from_secs(5), publisher_events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            NetworkEvent::Listening(address) => address,
            event => panic!("expected listen address, got {event:?}"),
        };

        let (follower, _follower_handle, mut follower_events) =
            NetworkService::new(local_config(0, vec![publish_addr])).unwrap();
        let follower_task = tokio::spawn(follower.run());

        timeout(Duration::from_secs(10), async {
            loop {
                if matches!(
                    publisher_events.recv().await,
                    Some(NetworkEvent::PeerSubscribed(_))
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();

        let expected = signed_message(&vec![0x5a; 8_192]);
        publish_handle.publish(42, expected.clone()).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match timeout(remaining, follower_events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                NetworkEvent::Message(message) => {
                    assert_eq!(message, expected);
                    break;
                }
                NetworkEvent::Listening(_)
                | NetworkEvent::PeerSubscribed(_)
                | NetworkEvent::BackfillUnavailable(_) => {}
            }
        }

        publisher_task.abort();
        follower_task.abort();
    }

    #[tokio::test]
    async fn subscribed_peer_can_backfill_cached_message() {
        let (publisher, publish_handle, mut publisher_events) =
            NetworkService::new(local_config(0, Vec::new())).unwrap();
        let publisher_task = tokio::spawn(publisher.run());
        let publish_addr = match timeout(Duration::from_secs(5), publisher_events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            NetworkEvent::Listening(address) => address,
            event => panic!("expected listen address, got {event:?}"),
        };
        let expected = signed_message(&vec![0xa5; 8_192]);
        publish_handle.publish(42, expected.clone()).await.unwrap();

        let (follower, follower_handle, mut follower_events) =
            NetworkService::new(local_config(0, vec![publish_addr])).unwrap();
        let follower_task = tokio::spawn(follower.run());
        timeout(Duration::from_secs(10), async {
            loop {
                if matches!(
                    follower_events.recv().await,
                    Some(NetworkEvent::PeerSubscribed(_))
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();

        follower_handle.request_payload(42).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match timeout(remaining, follower_events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                NetworkEvent::Message(message) => {
                    assert_eq!(message, expected);
                    break;
                }
                NetworkEvent::Listening(_)
                | NetworkEvent::PeerSubscribed(_)
                | NetworkEvent::BackfillUnavailable(_) => {}
            }
        }

        publisher_task.abort();
        follower_task.abort();
    }
}
