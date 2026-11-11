use std::process::Command;

// Completion must not load the cluster config, start the logger or build a
// runtime, so it works on a machine without a config file. Loading config
// prints "Warning: Failed to load config file" to stderr, so an empty stderr
// shows that the completion branch returned before that step.
#[test]
fn completion_runs_without_loading_config() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args(["--conf", "/nonexistent/curvine.toml", "completion", "bash"])
        .output()
        .expect("run curvine-cli completion bash");

    assert!(
        output.status.success(),
        "completion failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.stdout.is_empty(), "completion printed no script");
    assert!(
        output.stderr.is_empty(),
        "completion wrote to stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
