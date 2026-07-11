fn main() {
    pkg_config::Config::new()
        .atleast_version("0.8.36")
        .probe("aravis-0.8")
        .unwrap_or_else(|error| {
            panic!(
                "camera-adapter requires native Aravis >= 0.8.36 for interface-scoped discovery: {error}"
            )
        });
}
