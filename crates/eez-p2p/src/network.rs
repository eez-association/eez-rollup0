use std::time::Duration;

use futures_util::StreamExt;
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder,
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    identity, noise,
    swarm::SwarmEvent,
    tcp, yamux,
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{Level, event};

use crate::blocks_topic;

const COMMAND_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 128;
const REDIAL_INTERVAL: Duration = Duration::from_secs(5);

/// OP-compatible maximum uncompressed GossipSub block size.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Typed configuration for one chain-scoped unsafe-block network.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Rollup chain ID, used in the GossipSub topic.
    pub chain_id: u64,
    /// Local TCP listen address, for example `/ip4/0.0.0.0/tcp/9300`.
    pub listen_addr: Multiaddr,
    /// Static peers to dial. A `/p2p/<peer-id>` suffix is optional.
    pub peers: Vec<Multiaddr>,
}

impl NetworkConfig {
    /// Parse CLI/environment multiaddrs into a typed configuration.
    pub fn parse<'a>(
        chain_id: u64,
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
    Publish(Vec<u8>),
}

/// Clone-cheap command handle for a running network service.
#[derive(Debug, Clone)]
pub struct NetworkHandle {
    commands: mpsc::Sender<NetworkCommand>,
}

impl NetworkHandle {
    /// Queue one signed, uncompressed unsafe-block message for Snappy + GossipSub publication.
    pub async fn publish(&self, message: Vec<u8>) -> Result<(), NetworkError> {
        self.commands
            .send(NetworkCommand::Publish(message))
            .await
            .map_err(|_| NetworkError::ServiceStopped)
    }
}

/// Single-owner libp2p event loop.
pub struct NetworkService {
    swarm: Swarm<gossipsub::Behaviour>,
    topic: IdentTopic,
    peers: Vec<Multiaddr>,
    commands: mpsc::Receiver<NetworkCommand>,
    events: mpsc::Sender<NetworkEvent>,
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
        let identity = identity::Keypair::generate_ed25519();
        let message_id_fn = |message: &gossipsub::Message| {
            gossipsub::MessageId::from(Sha256::digest(&message.data).to_vec())
        };
        let gossip_config = gossipsub::ConfigBuilder::default()
            .validation_mode(ValidationMode::Strict)
            .max_transmit_size(MAX_MESSAGE_SIZE)
            .message_id_fn(message_id_fn)
            .build()
            .map_err(|error| NetworkError::GossipConfig(error.to_string()))?;
        let behaviour =
            gossipsub::Behaviour::new(MessageAuthenticity::Signed(identity.clone()), gossip_config)
                .map_err(|error| NetworkError::GossipConfig(error.to_string()))?;
        let mut swarm = SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
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
                    let Some(NetworkCommand::Publish(message)) = command else {
                        return;
                    };
                    self.publish(message);
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

    fn publish(&mut self, message: Vec<u8>) {
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
        match self
            .swarm
            .behaviour_mut()
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

    async fn on_swarm_event(&mut self, event: SwarmEvent<gossipsub::Event>) {
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
            SwarmEvent::Behaviour(gossipsub::Event::Subscribed { peer_id, topic })
                if topic == self.topic.hash() =>
            {
                let _ = self
                    .events
                    .send(NetworkEvent::PeerSubscribed(peer_id))
                    .await;
            }
            SwarmEvent::Behaviour(gossipsub::Event::Message { message, .. }) => {
                let message = match decompress(&message.data) {
                    Ok(message) => message,
                    Err(error) => {
                        event!(
                            name: "eez.p2p.block.decode_rejected",
                            Level::WARN,
                            %error,
                            "rejected malformed Snappy unsafe-block message",
                        );
                        return;
                    }
                };
                let _ = self.events.send(NetworkEvent::Message(message)).await;
            }
            _ => {}
        }
    }
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
    use tokio::time::{Instant, timeout};

    fn local_config(port: u16, peers: Vec<Multiaddr>) -> NetworkConfig {
        NetworkConfig {
            chain_id: 1234,
            listen_addr: format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap(),
            peers,
        }
    }

    #[test]
    fn snappy_roundtrip_and_size_limit() {
        let message = vec![0x42; 32_000];
        assert_eq!(decompress(&compress(&message).unwrap()).unwrap(), message);
        assert!(compress(&vec![0; MAX_MESSAGE_SIZE + 1]).is_err());
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

        let expected = vec![0x5a; 8_192];
        publish_handle.publish(expected.clone()).await.unwrap();
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
                NetworkEvent::Listening(_) | NetworkEvent::PeerSubscribed(_) => {}
            }
        }

        publisher_task.abort();
        follower_task.abort();
    }
}
