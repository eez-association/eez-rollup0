//! tonic-prost-build hook: compiles `proto/control.proto` into
//! `$OUT_DIR/control.v1.rs`, pulled in via `include!` from
//! `src/generated.rs`. Requires `protoc` on PATH.

fn main() {
    println!("cargo:rerun-if-changed=proto/control.proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/control.proto"], &["proto"])
        .expect("tonic-prost-build failed to compile control.proto");
}
