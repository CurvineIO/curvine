//! `cv completion <shell>` must work on a machine with no config and no cluster:
//! it only prints a script, so it must be handled before configuration is
//! resolved. The flags below are invalid on purpose.

use std::process::Command;

fn completion(shell: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args([
            "--conf",
            "/nonexistent/cv.toml",
            "--master-addrs",
            "127.0.0.1:1",
            "completion",
            shell,
        ])
        .output()
        .expect("run curvine-cli completion")
}

#[test]
fn completion_does_not_touch_configuration_or_a_cluster() {
    for shell in ["bash", "zsh", "fish", "powershell"] {
        let output = completion(shell);
        assert!(
            output.status.success(),
            "{shell} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.stdout.is_empty(), "{shell}: no script on stdout");
        assert!(
            output.stderr.is_empty(),
            "{shell}: completion must not log anything: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn the_script_matches_the_requested_shell() {
    let zsh = String::from_utf8_lossy(&completion("zsh").stdout).to_string();
    assert!(zsh.starts_with("#compdef cv"), "{zsh}");
}
