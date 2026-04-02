fn main() {
    let proto = "proto/autumn.proto";
    println!("cargo:rerun-if-changed={proto}");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out.join("autumn_descriptor.bin"))
        // Use bytes::Bytes for `bytes` fields in streaming RPC messages.
        // Bytes::clone() is O(1) ref-count bump instead of a full memcpy,
        // eliminating redundant payload copies on the append hot path.
        .bytes(&[
            ".autumn.v1.AppendRequest",
            ".autumn.v1.ReadBytesResponse",
            ".autumn.v1.CopyExtentResponse",
            ".autumn.v1.StreamPutRequest",
            ".autumn.v1.Payload",
        ])
        .compile(&[proto], &["proto"])
        .expect("compile proto");
}
