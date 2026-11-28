use std::path::Path;
use std::process::{Command, Output};

fn run_put(local_path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .env_remove("CURVINE_CONF_FILE")
        .args(["fs", "put"])
        .arg(local_path)
        .arg("/curvine-cli-exit-status-test")
        .output()
        .expect("run curvine-cli fs put")
}

#[test]
fn put_missing_local_file_exits_with_failure() {
    let missing = std::env::temp_dir().join(format!("curvine-cli-missing-{}", std::process::id()));
    assert!(!missing.exists());

    let output = run_put(&missing);
    assert!(
        !output.status.success(),
        "put of a missing local file must fail, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
}

#[test]
fn put_local_directory_exits_with_failure() {
    let dir = std::env::temp_dir();
    assert!(dir.is_dir());

    let output = run_put(&dir);
    assert!(
        !output.status.success(),
        "put of a local directory must fail, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("is not a file"));
}

#[test]
fn put_failure_is_reported_once() {
    let missing =
        std::env::temp_dir().join(format!("curvine-cli-reported-once-{}", std::process::id()));
    assert!(!missing.exists());

    let output = run_put(&missing);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert_eq!(
        stderr.matches("does not exist").count(),
        1,
        "the failure must be printed exactly once, stderr: {}",
        stderr
    );
    assert!(
        stderr.contains(&format!(
            "Error: Local file '{}' does not exist",
            missing.display()
        )),
        "the failure must be printed in Display form, stderr: {}",
        stderr
    );
}

#[test]
fn pre_runtime_failure_is_reported_in_display_form() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .env_remove("CURVINE_CONF_FILE")
        .args(["--master-addrs", "bogus", "fs", "ls", "/"])
        .output()
        .expect("run curvine-cli with invalid --master-addrs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert_eq!(
        stderr
            .matches("Invalid master_addrs format: 'bogus'")
            .count(),
        1,
        "the failure must be printed exactly once, stderr: {}",
        stderr
    );
    assert!(
        !stderr.contains("Error: \""),
        "the failure must not be printed in Debug form, stderr: {}",
        stderr
    );
}
