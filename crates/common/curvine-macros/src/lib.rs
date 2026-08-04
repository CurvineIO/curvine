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

//! Proc-macros for Curvine configuration structs under `curvine-common`.

mod client_cli;

use proc_macro::TokenStream;

/// Derives `{Type}CliOverrides` with clap flags and `apply_to` for `#[client_cli]` fields.
///
/// Container-level attributes (on the struct):
/// - `#[client_cli(prefix = "client")]` — prepended to generated long names (e.g. `client.io-threads`)
/// - `#[client_cli(strip_suffix = "_str")]` — strips a suffix from field names before kebab-case
/// - `#[client_cli(opt_in)]` — only fields marked with `#[client_cli]` are exposed (for incremental rollout)
///
/// Field-level attributes:
/// - `#[client_cli]` or `#[client_cli()]` — include field when `opt_in` is set
/// - `#[client_cli(skip)]` — omit from generated overrides
/// - `#[client_cli(long = "...")]` — explicit clap long name
///
/// Fields marked `#[serde(skip)]` are skipped automatically.
#[proc_macro_derive(ClientCliArgs, attributes(client_cli))]
pub fn derive_client_cli_args(input: TokenStream) -> TokenStream {
    client_cli::derive_client_cli_args(input)
}
