fn main() {
    let proto_root = "../../proto";
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src")
        .compile(&["pspb.proto"], &[proto_root]).expect("Failed to compile protos");
}