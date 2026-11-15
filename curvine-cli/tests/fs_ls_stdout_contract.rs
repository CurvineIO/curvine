use std::process::Command;

#[test]
fn ls_help_documents_the_stdout_contract() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args(["fs", "ls", "--help"])
        .output()
        .expect("run curvine-cli fs ls --help");

    assert!(
        output.status.success(),
        "help command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("stdout"),
        "fs ls --help must say what stdout carries: {stdout}"
    );
    assert!(
        stdout.contains("stderr"),
        "fs ls --help must say where the summary line goes: {stdout}"
    );
}
