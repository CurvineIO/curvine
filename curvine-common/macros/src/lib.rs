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

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput};

/// Derives a `{Type}CliOverrides` companion struct and a no-op `apply_to` impl.
///
/// Field-level `#[client_cli(...)]` attributes will be handled in a later commit.
#[proc_macro_derive(ClientCliArgs, attributes(client_cli))]
pub fn derive_client_cli_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    if !matches!(input.data, Data::Struct(_)) {
        return syn::Error::new_spanned(
            &input.ident,
            "ClientCliArgs can only be derived for structs",
        )
        .to_compile_error()
        .into();
    }

    let conf_name = &input.ident;
    let overrides_name = format_ident!("{}CliOverrides", conf_name);

    let expanded = quote! {
        #[derive(Debug, Default, Clone, PartialEq, Eq)]
        pub struct #overrides_name {}

        impl #overrides_name {
            /// Applies CLI overrides onto the target configuration struct.
            pub fn apply_to(&self, _target: &mut #conf_name) -> ::orpc::CommonResult<()> {
                Ok(())
            }
        }
    };

    expanded.into()
}
