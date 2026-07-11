fn main() {
    if let Err(error) = camera_adapter::backend::genicam_aravis::discovery_helper_main() {
        eprintln!("genicam discovery helper failed: {}", error.safe_summary());
        std::process::exit(1);
    }
}
