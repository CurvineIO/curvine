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

use curvine_model::{DirectoryAttributeDelta, DirectoryAttributes};
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
    for operand in operands {
        let delta = DirectoryAttributeDelta::decode(operand)?;
        if attributes.is_none() {
            let base = delta.base()?;
            attributes = Some(base);
            initial_base = Some(base);
        } else if let (Some(base), Some(candidate)) = (initial_base, delta.base()) {
            if candidate != base {
                return None;
            }
        }
        attributes.as_mut()?.apply(delta)?;
    }
    Some(attributes?.encode().to_vec())
}

fn merge_deltas(_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut combined = match existing {
        Some(value) => Some(DirectoryAttributeDelta::decode(value)?),
        None => None,
    };
    for operand in operands {
        let delta = DirectoryAttributeDelta::decode(operand)?;
        combined = Some(match combined {
            Some(previous) => previous.combine(delta)?,
            None => delta,
        });
    }
    Some(combined?.encode().to_vec())
}
