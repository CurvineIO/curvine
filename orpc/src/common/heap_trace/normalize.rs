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

use crate::common::Utils;
use crate::CommonResult;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;

pub const UNKNOWN_SITE_NAME: &str = "unknown";
pub const OTHER_HOTSPOT_SITE_NAME: &str = "__other__";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct NormalizedHeapSite {
    pub site_name: String,
    pub stable_id: String,
    pub frames: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapTraceHotspot {
    pub rank: usize,
    pub site_name: String,
    pub stable_id: String,
    pub bytes: usize,
    pub frames: Vec<String>,
}

pub fn normalize_stack_lines(input: &str) -> CommonResult<String> {
    let frames = input
        .lines()
        .map(normalize_frame)
        .filter(|frame| !frame.is_empty())
        .collect::<Vec<_>>();
    Ok(frames.join("\n"))
}

pub fn normalize_site(frames: &[String]) -> NormalizedHeapSite {
    let frames = frames
        .iter()
        .map(|frame| normalize_frame(frame))
        .filter(|frame| !frame.is_empty())
        .collect::<Vec<_>>();
    let site_name = select_site_name(&frames);
    let stable_id = stable_site_id(&frames);
    NormalizedHeapSite {
        site_name,
        stable_id,
        frames,
    }
}

pub fn reduce_topn(mut hotspots: Vec<HeapTraceHotspot>, topn: usize) -> Vec<HeapTraceHotspot> {
    hotspots.sort_by_key(|hotspot| {
        (
            Reverse(hotspot.bytes),
            hotspot.site_name.clone(),
            hotspot.stable_id.clone(),
        )
    });

    if topn == 0 {
        let other = aggregate_other(hotspots);
        return if other.bytes == 0 {
            Vec::new()
        } else {
            vec![HeapTraceHotspot { rank: 1, ..other }]
        };
    }

    let split_at = hotspots.len().min(topn);
    let remaining = hotspots.split_off(split_at);
    let mut reduced = hotspots;

    for (idx, hotspot) in reduced.iter_mut().enumerate() {
        hotspot.rank = idx + 1;
    }

    let other = aggregate_other(remaining);
    if other.bytes > 0 {
        reduced.push(HeapTraceHotspot {
            rank: reduced.len() + 1,
            ..other
        });
    }

    reduced
}

fn aggregate_other(hotspots: Vec<HeapTraceHotspot>) -> HeapTraceHotspot {
    let bytes = hotspots.into_iter().map(|hotspot| hotspot.bytes).sum();
    HeapTraceHotspot {
        rank: 0,
        site_name: OTHER_HOTSPOT_SITE_NAME.to_string(),
        stable_id: Utils::md5(OTHER_HOTSPOT_SITE_NAME),
        bytes,
        frames: vec![OTHER_HOTSPOT_SITE_NAME.to_string()],
    }
}

fn stable_site_id(frames: &[String]) -> String {
    if frames.is_empty() {
        return Utils::md5(UNKNOWN_SITE_NAME);
    }
    Utils::md5(frames.join("|").as_str())
}

fn select_site_name(frames: &[String]) -> String {
    for frame in frames {
        let site_name = extract_site_name(frame);
        if site_name.contains("curvine_") {
            return site_name;
        }
    }

    for frame in frames {
        let site_name = extract_site_name(frame);
        if site_name.contains("rocksdb::") {
            return site_name;
        }
    }

    frames
        .first()
        .map(|frame| extract_site_name(frame))
        .unwrap_or_else(|| UNKNOWN_SITE_NAME.to_string())
}

fn extract_site_name(frame: &str) -> String {
    let symbol = frame.split(" at ").next().unwrap_or(frame).trim();
    strip_frame_index(symbol).to_string()
}

fn strip_frame_index(frame: &str) -> &str {
    if let Some((prefix, rest)) = frame.split_once(':') {
        if prefix.chars().all(|c| c.is_ascii_digit()) {
            return rest.trim();
        }
    }
    frame
}

fn normalize_frame(frame: &str) -> String {
    let frame = strip_frame_index(frame.trim());
    let frame = strip_hex_addresses(frame);
    let frame = strip_line_numbers(&frame);
    collapse_whitespace(&frame)
}

fn strip_hex_addresses(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len());
    let chars = input.chars().collect::<Vec<_>>();
    let mut idx = 0;

    while idx < chars.len() {
        if chars[idx] == '0'
            && idx + 2 <= chars.len()
            && chars.get(idx + 1) == Some(&'x')
            && chars.get(idx + 2).is_some_and(|ch| ch.is_ascii_hexdigit())
        {
            idx += 2;
            while idx < chars.len() && chars[idx].is_ascii_hexdigit() {
                idx += 1;
            }
            continue;
        }

        normalized.push(chars[idx]);
        idx += 1;
    }

    normalized
}

fn strip_line_numbers(input: &str) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut normalized = String::with_capacity(chars.len());
    let mut idx = 0;

    while idx < chars.len() {
        if chars[idx] == ':'
            && chars.get(idx + 1).is_some_and(|ch| ch.is_ascii_digit())
            && chars.get(idx.wrapping_sub(1)) != Some(&':')
        {
            idx += 1;
            while idx < chars.len() && chars[idx].is_ascii_digit() {
                idx += 1;
            }
            continue;
        }

        normalized.push(chars[idx]);
        idx += 1;
    }

    normalized.trim().to_string()
}

fn collapse_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}
