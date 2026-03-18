fn main() {
    let proto = "proto/autumn.proto";
    println!("cargo:rerun-if-changed={proto}");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out.join("autumn_descriptor.bin"))
        .compile(&[proto], &["proto"])
        .expect("compile proto");
}
