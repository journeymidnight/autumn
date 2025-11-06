fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only compile pb.proto since it doesn't have google/api dependencies
    let proto_files = vec![
        "../../proto/pb.proto",
    ];
    
    let includes = vec!["../../proto"];
    
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&proto_files, &includes)?;
    
    // Re-run if proto files change
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }
    
    Ok(())
}