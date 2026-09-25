//! tonic-prost-build hook: compiles `proto/prove.proto` into
//! `$OUT_DIR/prove.v1.rs`, pulled in via `include!` from `src/generated.rs`.

fn main() {
    println!("cargo:rerun-if-changed=proto/prove.proto");

    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("failed to locate the Cargo-vendored protoc binary");
    let mut prost_config = tonic_prost_build::Config::new();
    prost_config.protoc_executable(protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(prost_config, &["proto/prove.proto"], &["proto"])
        .expect("tonic-prost-build failed to compile prove.proto");
}
