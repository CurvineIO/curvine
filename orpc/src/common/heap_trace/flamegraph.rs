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

use crate::CommonResult;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const FLAMEGRAPH_FORMAT: &str = "svg";
const FLAMEGRAPH_TITLE: &str = "Curvine Heap Trace";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapTraceFlamegraph {
    pub format: String,
    pub svg: String,
}

impl HeapTraceFlamegraph {
    pub fn new(svg: String) -> Self {
        Self {
            format: FLAMEGRAPH_FORMAT.to_string(),
            svg,
        }
    }
}

pub fn write_flamegraph(
    flamegraph_path: &Path,
    collapsed_stacks: &str,
) -> CommonResult<HeapTraceFlamegraph> {
    let svg = render_flamegraph_svg(collapsed_stacks)?;
    if let Some(parent) = flamegraph_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(flamegraph_path, svg.as_bytes())?;
    Ok(HeapTraceFlamegraph::new(svg))
}

#[cfg(feature = "heap-trace")]
pub fn render_flamegraph_svg(collapsed_stacks: &str) -> CommonResult<String> {
    use inferno::flamegraph::{from_lines, Options};

    let mut options = Options::default();
    options.title = FLAMEGRAPH_TITLE.to_string();
    options.count_name = "bytes".to_string();
    options.no_sort = false;
    options.deterministic = true;

    let mut lines = collapsed_stacks
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    lines.sort_unstable();

    let mut output = Vec::new();
    from_lines(&mut options, lines.into_iter(), &mut output)?;
    Ok(String::from_utf8(output)?)
}

#[cfg(not(feature = "heap-trace"))]
pub fn render_flamegraph_svg(collapsed_stacks: &str) -> CommonResult<String> {
    let mut svg = String::from(r#"<svg xmlns="http://www.w3.org/2000/svg" version="1.1">"#);
    svg.push_str("<title>");
    svg.push_str(FLAMEGRAPH_TITLE);
    svg.push_str("</title><desc>");
    svg.push_str(collapsed_stacks);
    svg.push_str("</desc></svg>");
    Ok(svg)
}

#[cfg(test)]
mod tests {
    use super::render_flamegraph_svg;

    #[test]
    fn render_flamegraph_from_collapsed_stacks() {
        let svg = render_flamegraph_svg("root;worker;alloc 5\nroot;worker;free 3\n").unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.contains("Curvine Heap Trace"));
    }
}
