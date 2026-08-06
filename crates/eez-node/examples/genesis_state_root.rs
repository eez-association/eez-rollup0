use std::{env, fs::File, path::PathBuf};

fn main() -> eyre::Result<()> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| eyre::eyre!("usage: genesis_state_root <genesis.json>"))?;
    let genesis: alloy_genesis::Genesis = serde_json::from_reader(File::open(&path)?)?;
    let chain_spec: reth_chainspec::ChainSpec = genesis.into();
    println!("{}", chain_spec.genesis_header().state_root);
    Ok(())
}
