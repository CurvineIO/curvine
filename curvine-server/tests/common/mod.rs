/// Skip SPDK integration tests when required env vars are not set.  
pub fn require_spdk_test_env() {
    let required = ["SPDK_TARGET_ADDR", "SPDK_TARGET_PORT", "SPDK_TARGET_NQN"];
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|key| std::env::var(key).ok().filter(|v| !v.is_empty()).is_none())
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "Skipping SPDK integration tests: set {} to run these tests.",
            missing.join(", ")
        );
        std::process::exit(0);
    }
}

/// IOVA mode for SPDK integration tests.
///
/// Defaults to `"va"` because VM/container/AMD hosts often fail auto-detect
/// (spdk/spdk#2683). Set `SPDK_IOVA_MODE` to override; use empty string to
/// exercise SPDK auto-detection.
pub fn spdk_iova_mode_for_test() -> String {
    match std::env::var("SPDK_IOVA_MODE") {
        Ok(v) => v,
        Err(_) => "va".to_string(),
    }
}
