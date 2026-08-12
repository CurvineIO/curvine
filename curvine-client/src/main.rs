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

use clap::Parser;
use curvine_sys::version;

#[derive(Debug, Parser)]
#[command(
    name = "curvine-client",
    version = version::VERSION,
    arg_required_else_help = true
)]
struct ClientArgs {
    /// Print the component version in JSON format and exit
    #[arg(long)]
    version_json: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = ClientArgs::parse();
    if args.version_json {
        println!("{}", version::component_version_json("client")?);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_json_parses_without_other_args() {
        let args = ClientArgs::try_parse_from(["curvine-client", "--version-json"])
            .expect("client version json should parse");

        assert!(args.version_json);
    }
}
