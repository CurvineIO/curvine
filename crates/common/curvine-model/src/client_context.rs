// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub const MAX_CLIENT_CONTEXT_KEYS: usize = 16;
pub const MAX_CLIENT_CONTEXT_KEY_LEN: usize = 64;
pub const MAX_CLIENT_CONTEXT_VALUE_LEN: usize = 256;

const DENYLISTED_KEY_SUBSTRINGS: &[&str] = &[
    "api_key",
    "access_key",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
];

/// Sorts, caps, and scrubs client context tags before they cross a trust
/// boundary (wire serialization or audit log output).
pub fn sanitize_client_context(
    tags: impl IntoIterator<Item = (String, String)>,
) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for (mut key, value) in tags {
        if !key.is_ascii() {
            continue;
        }
        if key.len() > MAX_CLIENT_CONTEXT_KEY_LEN {
            key.truncate(MAX_CLIENT_CONTEXT_KEY_LEN);
        }
        if !is_allowed_key(&key) || is_denylisted_key(&key) {
            continue;
        }

        let mut value = sanitize_value(&value);
        if value.len() > MAX_CLIENT_CONTEXT_VALUE_LEN {
            let mut end = MAX_CLIENT_CONTEXT_VALUE_LEN - 3;
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            value.truncate(end);
            value.push_str("...");
        }
        result.push((key, value));
    }

    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.truncate(MAX_CLIENT_CONTEXT_KEYS);
    result
}

fn is_allowed_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn is_denylisted_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    DENYLISTED_KEY_SUBSTRINGS
        .iter()
        .any(|denied| lower.contains(denied))
}

fn sanitize_value(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            ' ' | '=' | '\n' | '\r' | '\t' | '\0' => result.push('_'),
            c if c.is_control() => result.push('_'),
            c => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_and_drops_over_limit() {
        let tags = (0..20)
            .map(|i| (format!("key_{:02}", i), format!("v{}", i)))
            .collect::<Vec<_>>();
        let sanitized = sanitize_client_context(tags);

        assert_eq!(sanitized.len(), MAX_CLIENT_CONTEXT_KEYS);
        assert!(sanitized.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn drops_sensitive_and_invalid_keys() {
        let sanitized = sanitize_client_context(vec![
            ("app_id".to_string(), "job-1".to_string()),
            ("access_token".to_string(), "should-drop".to_string()),
            ("bad key".to_string(), "x".to_string()),
            ("owner".to_string(), "ops".to_string()),
        ]);

        assert_eq!(
            sanitized,
            vec![
                ("app_id".to_string(), "job-1".to_string()),
                ("owner".to_string(), "ops".to_string()),
            ]
        );
    }

    #[test]
    fn truncates_long_keys_and_values() {
        let long_key = "k".repeat(MAX_CLIENT_CONTEXT_KEY_LEN + 10);
        let long_value = "v".repeat(MAX_CLIENT_CONTEXT_VALUE_LEN + 10);
        let sanitized = sanitize_client_context(vec![(long_key.clone(), long_value)]);

        assert_eq!(sanitized.len(), 1);
        assert_eq!(sanitized[0].0.len(), MAX_CLIENT_CONTEXT_KEY_LEN);
        assert!(sanitized[0].1.ends_with("..."));
        assert!(sanitized[0].1.len() <= MAX_CLIENT_CONTEXT_VALUE_LEN);
    }

    #[test]
    fn truncates_multibyte_values_without_panicking() {
        let long_value = "你".repeat(MAX_CLIENT_CONTEXT_VALUE_LEN + 10);
        let sanitized = sanitize_client_context(vec![("scenario".to_string(), long_value)]);

        assert_eq!(sanitized.len(), 1);
        assert!(sanitized[0].1.ends_with("..."));
        assert!(sanitized[0].1.len() <= MAX_CLIENT_CONTEXT_VALUE_LEN);
        assert!(sanitized[0].1.is_char_boundary(sanitized[0].1.len()));
    }

    #[test]
    fn sanitizes_log_breaking_characters() {
        let sanitized = sanitize_client_context(vec![(
            "scenario".to_string(),
            "spark etl\nnext".to_string(),
        )]);

        assert_eq!(
            sanitized,
            vec![("scenario".to_string(), "spark_etl_next".to_string())]
        );
    }
}
