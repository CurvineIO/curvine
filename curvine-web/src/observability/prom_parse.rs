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

use std::collections::BTreeMap;

use super::model::MetricSample;

#[derive(Clone, Debug, Default)]
pub struct ParseResult {
    pub samples: Vec<MetricSample>,
    pub error_count: usize,
}

pub fn parse_prometheus_text(input: &str) -> ParseResult {
    let mut result = ParseResult::default();
    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match parse_line(line) {
            Some(sample) => result.samples.push(sample),
            None => result.error_count += 1,
        }
    }
    result
}

fn parse_line(line: &str) -> Option<MetricSample> {
    let line = line.trim();
    if let Some(end) = line.rfind('}') {
        let head = &line[..=end];
        let rest = line[end + 1..].trim_start();
        return build_sample_with_optional_timestamp(head, rest);
    }
    let parts: Vec<&str> = line.rsplitn(3, char::is_whitespace).collect();
    match parts.as_slice() {
        [timestamp, value, head]
            if is_optional_unix_timestamp(timestamp) && value.parse::<f64>().is_ok() =>
        {
            build_sample(head, value)
        }
        [value, head] => build_sample_with_optional_timestamp(head, value),
        _ => None,
    }
}

fn build_sample_with_optional_timestamp(head: &str, rest: &str) -> Option<MetricSample> {
    let mut tokens = rest.split_whitespace();
    let value = tokens.next()?;
    build_sample(head, value)
}

fn build_sample(head: &str, value: &str) -> Option<MetricSample> {
    let value = value.parse::<f64>().ok()?;
    let (name, labels) = parse_head(head)?;
    Some(MetricSample {
        name,
        labels,
        value,
    })
}

fn is_optional_unix_timestamp(token: &str) -> bool {
    token.len() >= 10 && token.chars().all(|c| c.is_ascii_digit())
}

fn parse_head(head: &str) -> Option<(String, BTreeMap<String, String>)> {
    let Some(label_start) = head.find('{') else {
        return valid_metric_name(head).then(|| (head.to_string(), BTreeMap::new()));
    };
    if !head.ends_with('}') || label_start == 0 {
        return None;
    }
    let name = &head[..label_start];
    if !valid_metric_name(name) {
        return None;
    }
    let label_text = &head[label_start + 1..head.len() - 1];
    Some((name.to_string(), parse_labels(label_text)?))
}

fn valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first == ':' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch == ':' || ch.is_ascii_alphanumeric())
}

fn parse_labels(input: &str) -> Option<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    let mut rest = input.trim();
    while !rest.is_empty() {
        let eq = rest.find('=')?;
        let key = rest[..eq].trim();
        if !valid_label_name(key) {
            return None;
        }
        rest = rest[eq + 1..].trim_start();
        if !rest.starts_with('"') {
            return None;
        }
        let (value, used) = parse_quoted_value(rest)?;
        labels.insert(key.to_string(), value);
        rest = rest[used..].trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if !rest.is_empty() {
            return None;
        }
    }
    Some(labels)
}

fn valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_quoted_value(input: &str) -> Option<(String, usize)> {
    let mut value = String::new();
    let mut escaped = false;
    for (idx, ch) in input.char_indices().skip(1) {
        if escaped {
            match ch {
                'n' => value.push('\n'),
                '"' => value.push('"'),
                '\\' => value.push('\\'),
                other => value.push(other),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some((value, idx + 1));
        } else {
            value.push(ch);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_metric() {
        let result = parse_prometheus_text("capacity 123\n");
        assert_eq!(result.error_count, 0);
        assert_eq!(result.samples[0].name, "capacity");
        assert_eq!(result.samples[0].value, 123.0);
    }

    #[test]
    fn parses_and_sorts_labels() {
        let result = parse_prometheus_text("worker_num{tag=\"live\",zone=\"a\"} 3\n");
        let labels: Vec<_> = result.samples[0].labels.keys().cloned().collect();
        assert_eq!(labels, vec!["tag".to_string(), "zone".to_string()]);
    }

    #[test]
    fn ignores_comments_and_counts_malformed_lines() {
        let result = parse_prometheus_text("# HELP capacity x\nbad line\ncapacity 1\n");
        assert_eq!(result.error_count, 1);
        assert_eq!(result.samples.len(), 1);
    }

    #[test]
    fn parses_label_values_with_spaces() {
        let result = parse_prometheus_text("metric{label=\"a b\"} 1\n");
        assert_eq!(result.error_count, 0);
        assert_eq!(result.samples[0].name, "metric");
        assert_eq!(result.samples[0].labels.get("label").unwrap(), "a b");
        assert_eq!(result.samples[0].value, 1.0);
    }

    #[test]
    fn ignores_optional_timestamp_suffix() {
        let result = parse_prometheus_text("capacity 123 1760000000000\n");
        assert_eq!(result.error_count, 0);
        assert_eq!(result.samples[0].value, 123.0);
    }

    #[test]
    fn parses_histogram_bucket_sum_and_count_as_ordinary_series() {
        let input = "\
operation_duration_bucket{operation=\"read\",le=\"0.1\"} 4
operation_duration_sum{operation=\"read\"} 0.42
operation_duration_count{operation=\"read\"} 4
";
        let result = parse_prometheus_text(input);
        assert_eq!(result.error_count, 0);
        assert_eq!(result.samples.len(), 3);
        assert_eq!(result.samples[0].name, "operation_duration_bucket");
        assert_eq!(result.samples[1].name, "operation_duration_sum");
        assert_eq!(result.samples[2].name, "operation_duration_count");
    }
}
