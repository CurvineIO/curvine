// src/master/meta/glob_utils.rs
use glob::Pattern;

/// Returns `true` + compiled `Pattern` if valid glob, `false` + `None` otherwise
pub fn parse_glob_pattern(s: &str) -> (bool, Option<Pattern>) {
    // Fast heuristic: check common metachars first
    if s.contains(['*', '?', '[', '{', '\\']) {
        // Double-check with actual Pattern compilation
        match Pattern::new(s) {
            Ok(pattern) => (true, Some(pattern)),
            Err(_) => (false, None),
        }
    } else {
        (false, None)
    }
}
