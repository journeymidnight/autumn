use std::path::Path;
use git2::{ErrorCode, Repository};



fn main() {
    match Repository::clone(
        "https://github.com/googleapis/googleapis.git",
        Path::new("googleapis"),
    ) {
        Ok(_) => println!("Successfully cloned googleapis"),
        Err(e) => match e.code() {
            ErrorCode::Exists => {
                println!("Repository already exists, skipping clone")
            }
            _ => panic!("Error: {}", e),
        },
    }
    let proto_root = "../../proto";
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src")
        .compile(&["pspb.proto"], &[proto_root, "googleapis"]).expect("Failed to compile protos");
    
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src")
        .compile(&["pb.proto"], &[proto_root, "googleapis"]).expect("Failed to compile protos");
}