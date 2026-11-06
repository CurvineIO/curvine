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

use curvine_model::{is_legacy_record, DirectoryAttributeDelta, DirectoryAttributes};
use rocksdb::{MergeOperands, Options};

/// Configures a column family dedicated to directory attribute records.
pub fn configure_directory_attributes_merge(options: &mut Options) {
    options.set_merge_operator(
        "curvine-directory-attributes-v1",
        merge_attributes,
        merge_deltas,
    );
}

fn merge_attributes(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut attributes = match existing {
        Some(value) => Some(DirectoryAttributes::decode(value)?),
        None => None,
    };
    let mut initial_base = None;
    let mut legacy_format = existing.map(is_legacy_record);
    for operand in operands {
        // With no materialized value the first operand creates the record;
        // subsequent operands must not silently upgrade its generation.
        legacy_format.get_or_insert_with(|| is_legacy_record(operand));
        let delta = DirectoryAttributeDelta::decode(operand)?;
        if attributes.is_none() {
            let base = delta.base()?;
            attributes = Some(base);
            initial_base = Some(base);
        } else if let (Some(base), Some(mut candidate)) = (initial_base, delta.base()) {
            // Legacy deltas cannot encode a base count. Compare the fields
            // they actually carry, but still reject two conflicting counts.
            if candidate.children.is_none() || base.children.is_none() {
                candidate.children = base.children;
            }
            if candidate != base {
                return None;
            }
        }
        // A legacy operand does not say whether a directory edge changed.
        // Once such an operand reaches a v4 base, the count cannot remain
        // authoritative; readers must fall back to scanning the edges.
        if is_legacy_record(operand) {
            attributes.as_mut()?.children = None;
        }
        attributes.as_mut()?.apply(delta)?;
    }
    let attributes = attributes?;
    Some(if legacy_format == Some(false) {
        attributes.encode().to_vec()
    } else {
        attributes.encode_legacy().to_vec()
    })
}

fn merge_deltas(_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut combined = match existing {
        Some(value) => Some(DirectoryAttributeDelta::decode(value)?),
        None => None,
    };
    let mut format = existing.map(is_legacy_record);
    for operand in operands {
        let legacy = is_legacy_record(operand);
        if format.is_some_and(|previous| previous != legacy) {
            // Keep mixed generations separate: collapsing them would erase
            // the legacy operand that invalidates a v4 base's child count.
            return None;
        }
        format = Some(legacy);
        let delta = DirectoryAttributeDelta::decode(operand)?;
        combined = Some(match combined {
            Some(previous) => previous.combine(delta)?,
            None => delta,
        });
    }
    let combined = combined?;
    Some(if format == Some(false) {
        combined.encode().to_vec()
    } else {
        combined.encode_legacy().to_vec()
    })
}
