use std::{
    env, fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use eez_seq_sig::Envelope;
use eyre::{WrapErr, bail, eyre};
use futures_util::StreamExt;
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder,
    connection_limits::{self, ConnectionLimits},
    gossipsub::{
        self, IdentTopic, MessageAcceptance, MessageAuthenticity, MessageId, PublishError,
        ValidationMode,
    },
    identity::Keypair,
    multiaddr::Protocol,
    noise,
    swarm::{
        DialError, NetworkBehaviour, SwarmEvent,
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
/// Gossipsub's open substreams keep every connection alive, so this only closes
/// connections that never open one, such as peers without gossipsub.
const IDLE_TIMEOUT: Duration = Duration::from_mins(1);
/// Discovered peers are dialed only below this many connections; past gossipsub's
/// `mesh_n_high` (12) more peers add churn, not reach. Static peers are always dialed.
const DISCOVERY_PEER_LIMIT: usize = 16;
/// Caps how many peers the sequencer flood-publishes each head to.
const MAX_INBOUND_PEERS: u32 = 64;

/// Overlay settings: where to listen, whom to dial, which topic to join.
#[derive(Clone, Debug)]
pub struct P2pConfig {
    pub listen: Multiaddr,
    /// Static peers, each ending in `/p2p/<id>`.
    pub peers: Vec<Multiaddr>,
    pub topic: String,
}

impl P2pConfig {
    /// `None` when `EEZ_P2P_LISTEN` is unset; Err on a bad listen address or peer.
    pub fn from_env(l2_chain_id: u64, rollup_id: u64) -> eyre::Result<Option<Self>> {
        let Some(listen) = listen_from_env()? else {
            return Ok(None);
        };
        Ok(Some(Self {
            listen,
            peers: peers_from_env()?,
            topic: topic(l2_chain_id, rollup_id),
        }))
    }
}

/// TCP port of `EEZ_P2P_LISTEN`; `None` when unset.
pub fn listen_port_from_env() -> eyre::Result<Option<u16>> {
    listen_from_env()?.as_ref().map(tcp_port_of).transpose()
}

/// Static peers from `EEZ_P2P_PEERS`, each ending in `/p2p/<id>`.
pub fn peers_from_env() -> eyre::Result<Vec<Multiaddr>> {
    var(PEERS_ENV)?
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
        .map(parse_peer)
        .collect()
}

pub fn topic(l2_chain_id: u64, rollup_id: u64) -> String {
    format!("/eez/{l2_chain_id}/{rollup_id}/heads/0")
}

/// Overlay addresses of discovered peers, each ending in `/p2p/<id>`.
pub type Discover = Box<dyn Fn() -> Vec<Multiaddr> + Send>;

/// Gossipsub, plus a cap on inbound connections.
#[derive(NetworkBehaviour)]
struct Behaviour {
    gossip: gossipsub::Behaviour,
    limits: connection_limits::Behaviour,
}

/// Gossipsub swarm that validates every head before relaying it.
pub struct Overlay {
    swarm: Swarm<Behaviour>,
    topic: IdentTopic,
    peers: Vec<Multiaddr>,
    validator: Validator,
    discover: Discover,
    /// Where the next tick starts in the discovered list.
    discovered_cursor: usize,
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
        let limits = connection_limits::Behaviour::new(
            ConnectionLimits::default()
                .with_max_established_incoming(Some(MAX_INBOUND_PEERS))
                .with_max_established_per_peer(Some(1)),
        );
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|_| Behaviour { gossip, limits })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(IDLE_TIMEOUT))
            .build();
        let topic = IdentTopic::new(config.topic);
        swarm.behaviour_mut().gossip.subscribe(&topic)?;
        swarm
            .listen_on(config.listen.clone())
            .wrap_err_with(|| format!("listen on {}", config.listen))?;
        Ok(Self {
            swarm,
            topic,
            peers: config.peers,
            validator,
            discover,
            discovered_cursor: 0,
            rejected: 0,
            last_reject: None,
        })
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
        for addr in &self.peers {
            dial(&mut self.swarm, addr);
        }
        let connected = self.swarm.network_info().num_peers();
        let budget = DISCOVERY_PEER_LIMIT.saturating_sub(connected);
        if budget == 0 {
            return;
        }
        let mut discovered = (self.discover)();
        discovered.retain(|addr| !peer_of(addr).is_some_and(|p| self.swarm.is_connected(&p)));
        for addr in next_batch(discovered, &mut self.discovered_cursor, budget) {
            dial(&mut self.swarm, &addr);
        }
    }

    fn publish(&mut self, head: &Envelope) {
        let number = head.head.number;
        let error = match wire::encode(head) {
            Ok(data) => match self
                .swarm
                .behaviour_mut()
                .gossip
                .publish(self.topic.clone(), data)
            {
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
        event: SwarmEvent<BehaviourEvent>,
        inbound: &mut Option<mpsc::Sender<Envelope>>,
    ) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Gossip(gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            })) => {
                let verdict = self.validator.check(&message.data, now_ms());
                let acceptance = match &verdict {
                    Verdict::Accept(_) => MessageAcceptance::Accept,
                    Verdict::Ignore => MessageAcceptance::Ignore,
                    Verdict::Reject(_) => MessageAcceptance::Reject,
                };
                // Reported before forwarding, so relay never depends on the local consumer;
                // gossipsub expires an unreported message from its cache within seconds.
                self.swarm
                    .behaviour_mut()
                    .gossip
                    .report_message_validation_result(&message_id, &propagation_source, acceptance);
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

/// Up to `budget` of `items` from `cursor` on, wrapping, and moves `cursor` past them,
/// so unreachable entries cannot take the whole budget on every tick.
fn next_batch<T>(mut items: Vec<T>, cursor: &mut usize, budget: usize) -> Vec<T> {
    if !items.is_empty() {
        let start = *cursor % items.len();
        items.rotate_left(start);
        items.truncate(budget);
        *cursor = cursor.wrapping_add(items.len());
    }
    items
}

fn dial(swarm: &mut Swarm<Behaviour>, addr: &Multiaddr) {
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
    // The transport has no DNS resolver, so such a peer would fail every dial.
    if !addr
        .iter()
        .any(|p| matches!(p, Protocol::Ip4(_) | Protocol::Ip6(_)))
    {
        bail!("{PEERS_ENV}: {raw}: DNS multiaddrs are not supported; use an IP");
    }
    Ok(addr)
}

fn listen_from_env() -> eyre::Result<Option<Multiaddr>> {
    var(LISTEN_ENV)?.map(|raw| parse_listen(&raw)).transpose()
}

fn parse_listen(raw: &str) -> eyre::Result<Multiaddr> {
    let addr = raw
        .trim()
        .parse()
        .wrap_err_with(|| format!("{LISTEN_ENV}: {raw}"))?;
    // The ENR advertises this port before the listener binds, so it must be fixed.
    if tcp_port_of(&addr)? == 0 {
        bail!("{LISTEN_ENV}: {raw} has TCP port 0; set a fixed port");
    }
    Ok(addr)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_port_zero_is_rejected() {
        assert!(parse_listen("/ip4/127.0.0.1/tcp/9100").is_ok());
        let err = parse_listen("/ip4/127.0.0.1/tcp/0").unwrap_err();
        assert!(err.to_string().contains("EEZ_P2P_LISTEN"), "{err}");
    }

    #[test]
    fn dns_peer_is_rejected() {
        let peer = crate::keypair(&[1; 32]).unwrap().public().to_peer_id();
        assert!(parse_peer(&format!("/ip4/10.0.0.1/tcp/9100/p2p/{peer}")).is_ok());
        let err = parse_peer(&format!("/dns4/seq.example/tcp/9100/p2p/{peer}")).unwrap_err();
        assert!(
            err.to_string().contains("DNS multiaddrs are not supported"),
            "{err}"
        );
    }

    #[test]
    fn discovered_batches_rotate() {
        let mut cursor = 0;
        assert_eq!(next_batch(vec![1, 2, 3], &mut cursor, 2), [1, 2]);
        assert_eq!(next_batch(vec![1, 2, 3], &mut cursor, 2), [3, 1]);
        assert_eq!(next_batch(vec![1, 2, 3], &mut cursor, 2), [2, 3]);
        assert!(next_batch(Vec::<u8>::new(), &mut cursor, 2).is_empty());
    }
}
