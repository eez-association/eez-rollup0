//! tonic-prost-build hook: compiles `proto/prove.proto` into
//! `$OUT_DIR/prove.v1.rs`, pulled in via `include!` from `src/generated.rs`.
//! Requires `protoc` on PATH.

fn main() {
    println!("cargo:rerun-if-changed=proto/prove.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/prove.proto"], &["proto"])
        .expect("tonic-prost-build failed to compile prove.proto");
}
