use std::process::Command;

#[test]
fn mkdir_and_touch_fail_without_writing_to_stdout() {
    let conf_dir = tempfile::tempdir().expect("create test config directory");
    let conf_path = conf_dir.path().join("curvine.toml");
    std::fs::write(
        &conf_path,
        "[client]\nrpc_retry_max_duration_ms = 1\nconn_timeout_ms = 100\nrpc_timeout_ms = 100\n\
         [cli.log]\nlog_dir = \"stderr\"\n",
    )
    .expect("write test config");

    for args in [
        ["fs", "mkdir", "/curvine-cli-output-test"],
        ["fs", "touch", "/curvine-cli-output-test"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
            .env_remove("CURVINE_CONF_FILE")
            .arg("--conf")
            .arg(&conf_path)
            .args(["--master-addrs", "127.0.0.1:1"])
            .args(args)
            .output()
            .expect("run curvine-cli fs mutation");

        assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
        assert!(
            output.stdout.is_empty(),
            "{args:?} wrote to stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("Error:"), "{args:?} stderr: {stderr}");
        assert!(
            !stderr.contains("Directory created successfully")
                && !stderr.contains("Created empty file:")
                && !stderr.contains("Updated timestamp:"),
            "{args:?} reported success on failure: {stderr}"
        );
    }
}
