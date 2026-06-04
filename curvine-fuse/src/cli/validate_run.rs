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

use crate::cli::FuseMountArgs;
use orpc::CommonResult;

/// Validates configuration by loading and initializing cluster settings without mounting.
pub fn run_validate_config(args: FuseMountArgs) -> CommonResult<()> {
    let mut conf = args.get_conf()?;
    conf.client.init()?;
    conf.print();
    eprintln!("Configuration validated successfully");
    Ok(())
}
