// src/master/meta/glob_utils.rs
use glob::Pattern;

/// Check if string contains glob pattern characters
pub fn is_glob_pattern(s: &str) -> bool {
    // Fast heuristic: check common metachars first
    if s.contains(|c| matches!(c, '*' | '?' | '[' | '{' | '\\')) {
        // Double-check with actual Pattern compilation
        Pattern::new(s).is_ok()
    } else {
        false
    }
}