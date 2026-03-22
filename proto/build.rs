fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../pilot/proto/regret.proto"], &["../pilot/proto"])?;
    Ok(())
}
