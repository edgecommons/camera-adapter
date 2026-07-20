# camera-adapter (Claude Code)

EdgeCommons southbound camera adapter (Rust), `com.mbreissi.edgecommons.CameraAdapter`. The full
picture — what this component is, the backend seam, config location, and the org conventions it
inherits — lives in `AGENTS.md` and is shared with every agent tool. It is imported here in full:

@AGENTS.md

## Local-dev notes

- **Sibling library override.** `Cargo.toml` pins the `edgecommons` dependency by git `rev`. For local
  development a gitignored `.cargo/config.toml` `[patch]` block redirects that pin at your sibling
  `core/libs/rust` checkout, so a plain `cargo build` tracks your working copy without touching the
  committed pin CI uses. `Cargo.lock` is committed and records the git source; a `[patch]`-ed build
  rewrites it to a path source locally — do not commit that churn.
- **Simulators.** `SimBackend` is compiled into every build and drives the deterministic suite with no
  hardware. The ONVIF/RTSP/GenICam simulators under `simulators/` back the live validation containers;
  `simulators/run-rtsp-native-coverage.ps1` runs the RTSP native-coverage harness (run it with the
  `.cargo` `[patch]` inactive so cargo never needs to rewrite the read-only-mounted committed lock).
- **Feature builds.** Default features are `standalone,onvif` (pure Rust, Windows-buildable). `rtsp`
  needs GStreamer and `genicam` needs Aravis ≥ 0.8.36 — build those in the simulator containers or WSL.
