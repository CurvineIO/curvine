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
