//! Advertises the overlay port in reth's discv5 ENR and reads it back from discovered peers.

use eez_p2p::Discover;
use libp2p::{Multiaddr, multiaddr::Protocol};
use reth_chainspec::{EnrForkIdEntry, ForkId, Hardforks};
use reth_discv5::{
    ConfigBuilder, NetworkStackId,
    discv5::enr::{Enr, EnrKey, EnrPublicKey},
};
use reth_network::{
    NetworkHandle, NetworkManager, NetworkPrimitives, PeersInfo, primitives::BasicNetworkPrimitives,
};
use reth_node_api::{FullNodeTypes, NodeTypes, PrimitivesTy, TxTy};
use reth_node_builder::{BuilderContext, components::NetworkBuilder};
use reth_transaction_pool::{PoolPooledTx, PoolTransaction, TransactionPool};
use tracing::{Level, event};

/// ENR key of the overlay entry; its value is the RLP of the overlay TCP port.
const ENR_KEY: &[u8] = b"eez";

/// Same as `EthereumNetworkBuilder`, plus the `eez` ENR entry when `EEZ_P2P_LISTEN` is set.
#[derive(Debug, Default, Clone, Copy)]
pub struct EezNetworkBuilder;

impl<Node, Pool> NetworkBuilder<Node, Pool> for EezNetworkBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec: Hardforks>>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<Node::Types>>>
        + Unpin
        + 'static,
{
    type Network =
        NetworkHandle<BasicNetworkPrimitives<PrimitivesTy<Node::Types>, PoolPooledTx<Pool>>>;

    async fn build_network(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<Self::Network> {
        let mut config = ctx.network_config()?;
        if let Some(port) = eez_p2p::listen_port_from_env()? {
            // No discv5 config means discovery is off; static EEZ_P2P_PEERS still work.
            match config.discovery_v5_config.take() {
                Some(discv5) => config.discovery_v5_config = Some(advertise(discv5, port)),
                None if eez_p2p::peers_from_env()?.is_empty() => event!(
                    name: "eez.p2p.no_peers",
                    Level::WARN,
                    "p2p overlay has no peers: discovery disabled and EEZ_P2P_PEERS empty",
                ),
                None => {}
            }
        }
        let handle = ctx.start_network(NetworkManager::builder(config).await?, pool);
        event!(
            name: "eez.node.network.started",
            Level::INFO,
            enode = %handle.local_node_record(),
            "P2P networking initialized",
        );
        Ok(handle)
    }
}

/// Adds the `eez` entry; discv5 stores the value as raw RLP.
fn advertise(config: reth_discv5::Config, port: u16) -> reth_discv5::Config {
    ConfigBuilder::new_from(config)
        .add_enr_kv_pair(ENR_KEY, alloy_rlp::encode(port).into())
        .build()
}

/// Overlay address of a discv5 entry, if it carries our entry and our fork id.
fn overlay_addr<K: EnrKey>(enr: &Enr<K>, our_fork: Option<ForkId>) -> Option<Multiaddr> {
    if fork_id(enr)? != our_fork? {
        return None;
    }
    let port = enr.get_decodable::<u16>(ENR_KEY)?.ok()?;
    let key: [u8; 33] = enr.public_key().encode().as_ref().try_into().ok()?;
    let peer = eez_p2p::peer_id(&key).ok()?;
    Some(
        Multiaddr::empty()
            .with(Protocol::Ip4(enr.ip4()?))
            .with(Protocol::Tcp(port))
            .with(Protocol::P2p(peer)),
    )
}

/// Reads the discv5 table; used as the overlay's `Discover` closure.
pub fn discover<N: NetworkPrimitives>(network: NetworkHandle<N>) -> Discover {
    Box::new(move || {
        let Some(discv5) = network.discv5() else {
            return Vec::new();
        };
        discv5.with_discv5(|d| {
            let ours = fork_id(&d.local_enr());
            d.table_entries_enr()
                .iter()
                .filter_map(|enr| overlay_addr(enr, ours))
                .collect()
        })
    })
}

/// reth writes a custom chain's fork id under `eth` (reth: net/network/src/config.rs:689-695).
fn fork_id<K: EnrKey>(enr: &Enr<K>) -> Option<ForkId> {
    Some(
        enr.get_decodable::<EnrForkIdEntry>(NetworkStackId::ETH)?
            .ok()?
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::ForkHash;
    use reth_discv5::{Config, build_local_enr, enr::EnrCombinedKeyWrapper};
    use reth_network::config::rng_secret_key;
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn overlay_addr_requires_entry_and_fork() {
        let ours = ForkId {
            hash: ForkHash([1, 2, 3, 4]),
            next: 0,
        };
        let theirs = ForkId { next: 9, ..ours };
        let sk = rng_secret_key();
        let base = Config::builder(SocketAddr::from((Ipv4Addr::LOCALHOST, 30303)))
            .advertised_ip(Ipv4Addr::LOCALHOST.into())
            .fork(NetworkStackId::ETH, ours)
            .build();
        // The discv5 table holds `Enr<CombinedKey>`, so test that key type.
        let enr = |c: &Config| EnrCombinedKeyWrapper::from(build_local_enr(&sk, c).0).0;
        let with_entry = enr(&advertise(base.clone(), 9100));
        let without_entry = enr(&base);
        let peer = eez_p2p::keypair(&sk.secret_bytes())
            .unwrap()
            .public()
            .to_peer_id();

        assert_eq!(
            overlay_addr(&with_entry, Some(ours)),
            Some(
                format!("/ip4/127.0.0.1/tcp/9100/p2p/{peer}")
                    .parse()
                    .unwrap()
            )
        );
        assert_eq!(overlay_addr(&without_entry, Some(ours)), None);
        assert_eq!(overlay_addr(&with_entry, Some(theirs)), None);
    }
}
