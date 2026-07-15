//! The sanitized panic hook keeps panic payloads out of the process's stderr.
//!
//! This lives in its own test binary on purpose. `install_sanitized_panic_hook` sets a PROCESS-WIDE
//! hook, so installing it inside the unit-test binary redacts the assertion message of every test in
//! it -- which is exactly what used to happen, and it made every CI failure unreadable.
//!
//! The property under test is a security one: a panic payload can carry a credential, a token, or a
//! frame of a customer's premises, and it must not reach stderr. The only honest way to prove that is
//! to panic in a real process and read what came out, which is what this does -- it re-executes itself
//! as a child, panics there with a payload nobody could mistake for anything else, and reads the
//! child's stderr.

use std::process::Command;

const SECRET: &str = "payload-that-must-never-reach-stderr";
const CHILD: &str = "CAMERA_ADAPTER_PANIC_HOOK_CHILD";

#[test]
fn the_panic_hook_keeps_the_payload_out_of_stderr() {
    if std::env::var(CHILD).is_ok() {
        camera_adapter::supervisor::install_sanitized_panic_hook();
        panic!("{SECRET}");
    }

    let executable = std::env::current_exe().expect("the test binary can re-run itself");
    let output = Command::new(executable)
        .args([
            "--exact",
            "the_panic_hook_keeps_the_payload_out_of_stderr",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .expect("the child runs");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains(SECRET),
        "the panic payload reached stderr, which is what the hook exists to prevent:\n{stderr}"
    );
    assert!(
        !output.status.success(),
        "the child must actually have panicked, or this test proves nothing"
    );
}
