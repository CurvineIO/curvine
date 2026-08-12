use std::process::Command;

#[test]
fn fluid_summary_subcommand_is_available() {
    let output = Command::new(env!("CARGO_BIN_EXE_curvine-cli"))
        .args(["report", "fluid-summary", "--help"])
        .output()
        .expect("run curvine-cli report fluid-summary --help");

    assert!(
        output.status.success(),
        "help command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Print Fluid CacheRuntime ReportSummary JSON"));
}
