//! In-process overlay nodes gossiping over loopback TCP.

use std::{
    net::TcpListener,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::Header;
use alloy_primitives::{Address, Bytes};
use alloy_signer_local::PrivateKeySigner;
use eez_p2p::{Overlay, P2pConfig, Validator, keypair, topic};
use eez_seq_sig::{Envelope, HeadDomain, HeadSigner, VerifiedHead};
use libp2p::Multiaddr;
use reth_primitives_traits::SealedHeader;
use tokio::{
    sync::{mpsc, watch},
    time::{sleep, timeout},
};

const DOMAIN: HeadDomain = HeadDomain {
    l2_chain_id: 1337,
    registry: Address::repeat_byte(0xaa),
    rollup_id: 1,
};
const DEADLINE: Duration = Duration::from_secs(30);
const HONEST_EVERY: Duration = Duration::from_millis(200);

fn key(i: u8) -> PrivateKeySigner {
    PrivateKeySigner::from_slice(&[i + 1; 32]).unwrap()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Signs `count` heads with key `signer`, one every `every`.
fn heads(signer: u8, every: Duration, count: u64) -> watch::Receiver<Option<Envelope>> {
    let (tx, rx) = watch::channel(None);
    let mut signer = HeadSigner::new(key(signer), DOMAIN, now_ms());
    tokio::spawn(async move {
        for number in 1..=count {
            let header = SealedHeader::seal_slow(Header {
                number,
                ..Default::default()
            });
            // An empty block goes over the wire as none: a bare signed head.
            let head = signer.sign(&header, Bytes::new()).unwrap();
            if tx.send(Some(head)).is_err() {
                return;
            }
            sleep(every).await;
        }
    });
    rx
}

/// Waits for a head with `counter >= 5`; every head must be signed by key 0.
async fn await_counter(rx: &mut mpsc::Receiver<VerifiedHead>) {
    timeout(DEADLINE, async {
        loop {
            let head = rx.recv().await.expect("inbound sender dropped");
            assert_eq!(
                head.signer,
                key(0).address(),
                "received a head not signed by key 0"
            );
            if head.claim.counter >= 5 {
                return;
            }
        }
    })
    .await
    .expect("no head with counter >= 5 within the deadline");
}

struct Node {
    secret: [u8; 32],
    port: u16,
    /// Index of the key this node's validator authorizes.
    authorized: u8,
}

impl Node {
    fn new(n: u8) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Self {
            secret: [n; 32],
            port,
            authorized: 0,
        }
    }

    fn addr(&self) -> Multiaddr {
        let id = keypair(&self.secret).unwrap().public().to_peer_id();
        format!("/ip4/127.0.0.1/tcp/{}/p2p/{id}", self.port)
            .parse()
            .unwrap()
    }

    fn spawn(
        &self,
        peers: &[&Node],
        publish: Option<watch::Receiver<Option<Envelope>>>,
        inbound: Option<mpsc::Sender<VerifiedHead>>,
    ) {
        let config = P2pConfig {
            listen: format!("/ip4/127.0.0.1/tcp/{}", self.port).parse().unwrap(),
            peers: peers.iter().map(|p| p.addr()).collect(),
            topic: topic(DOMAIN.l2_chain_id, DOMAIN.rollup_id),
        };
        let overlay = Overlay::new(
            config,
            keypair(&self.secret).unwrap(),
            Validator::new(DOMAIN, key(self.authorized).address()),
            Box::new(Vec::new),
        )
        .unwrap();
        tokio::spawn(overlay.run(publish, inbound));
    }
}

#[tokio::test]
async fn heads_relay_through_a_middle_node() {
    let (a, b, c) = (Node::new(1), Node::new(2), Node::new(3));
    // Never read: B's inbound stays full while it relays.
    let (b_tx, _b_rx) = mpsc::channel(1);
    let (c_tx, mut c_rx) = mpsc::channel(64);
    a.spawn(&[], Some(heads(0, HONEST_EVERY, u64::MAX)), None);
    b.spawn(&[&a], None, Some(b_tx));
    c.spawn(&[&b], None, Some(c_tx));
    await_counter(&mut c_rx).await;
}

#[tokio::test]
async fn rogue_flood_does_not_stall_the_publisher() {
    let (a, b, r) = (Node::new(1), Node::new(2), Node::new(3));
    // Accepts R's heads, so anything A relays from R would reach it.
    let probe = Node {
        authorized: 1,
        ..Node::new(4)
    };
    let (b_tx, mut b_rx) = mpsc::channel(64);
    let (probe_tx, mut probe_rx) = mpsc::channel(64);
    a.spawn(&[], Some(heads(0, HONEST_EVERY, u64::MAX)), None);
    b.spawn(&[&a], None, Some(b_tx));
    probe.spawn(&[&a], None, Some(probe_tx));
    r.spawn(&[&a], Some(heads(1, Duration::from_millis(2), 500)), None);
    await_counter(&mut b_rx).await;
    assert!(
        probe_rx.try_recv().is_err(),
        "A relayed a rejected key-1 head"
    );
}

#[tokio::test]
async fn late_peer_is_dialed() {
    let (a, b) = (Node::new(1), Node::new(2));
    let (b_tx, mut b_rx) = mpsc::channel(64);
    b.spawn(&[&a], None, Some(b_tx));
    sleep(Duration::from_secs(2)).await;
    a.spawn(&[], Some(heads(0, HONEST_EVERY, u64::MAX)), None);
    await_counter(&mut b_rx).await;
}
