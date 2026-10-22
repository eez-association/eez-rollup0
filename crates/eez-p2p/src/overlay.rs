use std::{
    env, fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use eez_seq_sig::Envelope;
use eyre::{WrapErr, eyre};
use futures_util::StreamExt;
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder,
    gossipsub::{
        self, IdentTopic, MessageAcceptance, MessageAuthenticity, MessageId, PublishError,
        ValidationMode,
    },
    identity::Keypair,
    multiaddr::Protocol,
    noise,
    swarm::{
        DialError, SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
    tcp, yamux,
};
use sha2::{Digest, Sha256};
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};
use tracing::{Level, event};

use crate::{MAX_MESSAGE_SIZE, Validator, Verdict, wire};

const LISTEN_ENV: &str = "EEZ_P2P_LISTEN";
const PEERS_ENV: &str = "EEZ_P2P_PEERS";
/// Peers are redialed, and rejects summarised, on this tick.
const DIAL_INTERVAL: Duration = Duration::from_secs(5);
/// Gossipsub keeps only mesh connections alive; this gives a new peer time to be grafted.
const IDLE_TIMEOUT: Duration = Duration::from_mins(1);
/// Discovered peers are dialed only below this many connections; past gossipsub's
/// `mesh_n_high` (12) more peers add churn, not reach. Static peers are always dialed.
const DISCOVERY_PEER_LIMIT: usize = 16;

/// Overlay settings: where to listen, whom to dial, which topic to join.
#[derive(Clone, Debug)]
pub struct P2pConfig {
    pub listen: Multiaddr,
    /// Static peers, each ending in `/p2p/<id>`.
    pub peers: Vec<Multiaddr>,
    pub topic: String,
}

impl P2pConfig {
    /// `None` when `EEZ_P2P_LISTEN` is unset; Err on a peer without `/p2p/<id>`.
    pub fn from_env(l2_chain_id: u64, rollup_id: u64) -> eyre::Result<Option<Self>> {
        let Some(listen) = listen_from_env()? else {
            return Ok(None);
        };
        let peers = var(PEERS_ENV)?
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .map(parse_peer)
            .collect::<eyre::Result<_>>()?;
        Ok(Some(Self {
            listen,
            peers,
            topic: topic(l2_chain_id, rollup_id),
        }))
    }

    pub fn tcp_port(&self) -> eyre::Result<u16> {
        tcp_port_of(&self.listen)
    }
}

/// TCP port of `EEZ_P2P_LISTEN`; `None` when unset.
pub fn listen_port_from_env() -> eyre::Result<Option<u16>> {
    listen_from_env()?.as_ref().map(tcp_port_of).transpose()
}

pub fn topic(l2_chain_id: u64, rollup_id: u64) -> String {
    format!("/eez/{l2_chain_id}/{rollup_id}/heads/0")
}

/// Overlay addresses of discovered peers, each ending in `/p2p/<id>`.
pub type Discover = Box<dyn Fn() -> Vec<Multiaddr> + Send>;

/// Gossipsub swarm that validates every head before relaying it.
pub struct Overlay {
    swarm: Swarm<gossipsub::Behaviour>,
    topic: IdentTopic,
    peers: Vec<Multiaddr>,
    validator: Validator,
    discover: Discover,
    rejected: u64,
    last_reject: Option<(PeerId, String)>,
}

impl fmt::Debug for Overlay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Overlay")
            .field("peer_id", self.swarm.local_peer_id())
            .field("topic", &self.topic)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

impl Overlay {
    /// Builds the swarm and starts listening. Call inside a Tokio runtime.
    pub fn new(
        config: P2pConfig,
        keypair: Keypair,
        validator: Validator,
        discover: Discover,
    ) -> eyre::Result<Self> {
        let gossip_config = gossipsub::ConfigBuilder::default()
            // The sequencer signature is the authentication, so messages carry no author.
            .validation_mode(ValidationMode::Anonymous)
            .validate_messages()
            .message_id_fn(|m| MessageId::from(Sha256::digest(&m.data).to_vec()))
            .max_transmit_size(MAX_MESSAGE_SIZE + 64 * 1024)
            .build()?;
        let gossip = gossipsub::Behaviour::new(MessageAuthenticity::Anonymous, gossip_config)
            .map_err(|e| eyre!("gossipsub: {e}"))?;
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|_| gossip)?
            .with_swarm_config(|c| c.with_idle_connection_timeout(IDLE_TIMEOUT))
            .build();
        let topic = IdentTopic::new(config.topic);
        swarm.behaviour_mut().subscribe(&topic)?;
        swarm
            .listen_on(config.listen.clone())
            .wrap_err_with(|| format!("listen on {}", config.listen))?;
        Ok(Self {
            swarm,
            topic,
            peers: config.peers,
            validator,
            discover,
            rejected: 0,
            last_reject: None,
        })
    }

    pub fn peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }

    /// Publishes each head from `publish` and forwards accepted heads to `inbound`.
    pub async fn run(
        mut self,
        mut publish: Option<watch::Receiver<Option<Envelope>>>,
        mut inbound: Option<mpsc::Sender<Envelope>>,
    ) {
        let mut dial = tokio::time::interval(DIAL_INTERVAL);
        loop {
            tokio::select! {
                _ = dial.tick() => {
                    self.dial_peers();
                    self.log_rejects();
                }
                head = next_head(&mut publish) => {
                    if let Some(head) = head {
                        self.publish(&head);
                    }
                }
                event = self.swarm.select_next_some() => self.on_event(event, &mut inbound),
            }
        }
    }

    fn dial_peers(&mut self) {
        let discovered = if self.swarm.network_info().num_peers() < DISCOVERY_PEER_LIMIT {
            (self.discover)()
        } else {
            Vec::new()
        };
        for addr in self.peers.iter().chain(&discovered) {
            dial(&mut self.swarm, addr);
        }
    }

    fn publish(&mut self, head: &Envelope) {
        let number = head.head.number;
        let error = match wire::encode(head) {
            Ok(data) => match self.swarm.behaviour_mut().publish(self.topic.clone(), data) {
                Ok(_) => return,
                Err(error @ (PublishError::Duplicate | PublishError::NoPeersSubscribedToTopic)) => {
                    event!(
                        name: "eez.p2p.publish.skipped",
                        Level::DEBUG,
                        number,
                        %error,
                        "head {{number}} not published: {{error}}",
                    );
                    return;
                }
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        };
        event!(
            name: "eez.p2p.publish.failed",
            Level::WARN,
            number,
            %error,
            "head {{number}} not published: {{error}}",
        );
    }

    fn on_event(
        &mut self,
        event: SwarmEvent<gossipsub::Event>,
        inbound: &mut Option<mpsc::Sender<Envelope>>,
    ) {
        match event {
            SwarmEvent::Behaviour(gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            }) => {
                let verdict = self.validator.check(&message.data, now_ms());
                let acceptance = match &verdict {
                    Verdict::Accept(_) => MessageAcceptance::Accept,
                    Verdict::Ignore => MessageAcceptance::Ignore,
                    Verdict::Reject(_) => MessageAcceptance::Reject,
                };
                // Reported before forwarding, so relay never depends on the local consumer;
                // gossipsub expires an unreported message from its cache within seconds.
                self.swarm.behaviour_mut().report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    acceptance,
                );
                match verdict {
                    Verdict::Accept(envelope) => forward(inbound, envelope),
                    Verdict::Ignore => {}
                    Verdict::Reject(reason) => {
                        event!(
                            name: "eez.p2p.message.rejected",
                            Level::DEBUG,
                            peer = %propagation_source,
                            %reason,
                            "rejected gossip from {{peer}}: {{reason}}",
                        );
                        self.rejected += 1;
                        self.last_reject = Some((propagation_source, reason));
                    }
                }
            }
            SwarmEvent::NewListenAddr { address, .. } => event!(
                name: "eez.p2p.listening",
                Level::INFO,
                %address,
                peer_id = %self.swarm.local_peer_id(),
                "overlay listening on {{address}} as {{peer_id}}",
            ),
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => event!(
                name: "eez.p2p.connection.failed",
                Level::DEBUG,
                peer = ?peer_id,
                %error,
                "overlay connection to {{peer}} failed: {{error}}",
            ),
            SwarmEvent::ListenerClosed {
                addresses,
                reason: Err(error),
                ..
            } => event!(
                name: "eez.p2p.listener.closed",
                Level::WARN,
                addresses = ?addresses,
                %error,
                "overlay listener on {{addresses}} closed: {{error}}",
            ),
            SwarmEvent::ListenerError { error, .. } => event!(
                name: "eez.p2p.listener.error",
                Level::WARN,
                %error,
                "overlay listener error: {{error}}",
            ),
            _ => {}
        }
    }

    /// One WARN per tick, so a rogue flood cannot flood the log.
    fn log_rejects(&mut self) {
        let Some((peer, reason)) = self.last_reject.take() else {
            return;
        };
        event!(
            name: "eez.p2p.message.rejects",
            Level::WARN,
            count = self.rejected,
            %peer,
            %reason,
            "rejected {{count}} gossip messages; last from {{peer}}: {{reason}}",
        );
        self.rejected = 0;
    }
}

/// Next head to publish; never resolves once the store is gone.
async fn next_head(publish: &mut Option<watch::Receiver<Option<Envelope>>>) -> Option<Envelope> {
    let Some(rx) = publish.as_mut() else {
        return std::future::pending().await;
    };
    if rx.changed().await.is_err() {
        event!(
            name: "eez.p2p.publish.closed",
            Level::WARN,
            "head store dropped; overlay stops publishing",
        );
        *publish = None;
        return None;
    }
    // Cheap: the signature and block are reference-counted bytes.
    rx.borrow_and_update().clone()
}

fn forward(inbound: &mut Option<mpsc::Sender<Envelope>>, envelope: Envelope) {
    let Some(tx) = inbound.as_ref() else {
        return;
    };
    match tx.try_send(envelope) {
        Ok(()) => {}
        Err(TrySendError::Full(envelope)) => event!(
            name: "eez.p2p.inbound.full",
            Level::WARN,
            number = envelope.head.number,
            counter = envelope.head.counter,
            "inbound head channel full; dropped head {{number}}",
        ),
        Err(TrySendError::Closed(_)) => {
            event!(
                name: "eez.p2p.inbound.closed",
                Level::WARN,
                "inbound head consumer closed; overlay stops forwarding",
            );
            *inbound = None;
        }
    }
}

fn dial(swarm: &mut Swarm<gossipsub::Behaviour>, addr: &Multiaddr) {
    let Some(peer) = peer_of(addr) else {
        event!(
            name: "eez.p2p.dial.no_peer_id",
            Level::WARN,
            %addr,
            "overlay peer {{addr}} does not end in /p2p/<id>; not dialing",
        );
        return;
    };
    // A bare multiaddr dial would open a new connection on every tick.
    let opts = DialOpts::peer_id(peer)
        .addresses(vec![addr.clone()])
        .condition(PeerCondition::DisconnectedAndNotDialing)
        .build();
    match swarm.dial(opts) {
        Ok(()) | Err(DialError::DialPeerConditionFalse(_)) => {}
        Err(error) => event!(
            name: "eez.p2p.dial.failed",
            Level::DEBUG,
            %peer,
            %error,
            "overlay dial of {{peer}} failed: {{error}}",
        ),
    }
}

fn peer_of(addr: &Multiaddr) -> Option<PeerId> {
    match addr.iter().last() {
        Some(Protocol::P2p(peer)) => Some(peer),
        _ => None,
    }
}

fn parse_peer(raw: &str) -> eyre::Result<Multiaddr> {
    let addr: Multiaddr = raw
        .parse()
        .wrap_err_with(|| format!("{PEERS_ENV}: {raw}"))?;
    peer_of(&addr).ok_or_else(|| eyre!("{PEERS_ENV}: {raw} does not end in /p2p/<id>"))?;
    Ok(addr)
}

fn listen_from_env() -> eyre::Result<Option<Multiaddr>> {
    let Some(raw) = var(LISTEN_ENV)? else {
        return Ok(None);
    };
    let addr = raw
        .trim()
        .parse()
        .wrap_err_with(|| format!("{LISTEN_ENV}: {raw}"))?;
    tcp_port_of(&addr)?;
    Ok(Some(addr))
}

fn tcp_port_of(addr: &Multiaddr) -> eyre::Result<u16> {
    addr.iter()
        .find_map(|p| match p {
            Protocol::Tcp(port) => Some(port),
            _ => None,
        })
        .ok_or_else(|| eyre!("{LISTEN_ENV}: {addr} is not a TCP multiaddr"))
}

fn var(name: &str) -> eyre::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(eyre!("{name}: {error}")),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
