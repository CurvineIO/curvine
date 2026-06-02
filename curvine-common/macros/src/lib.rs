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
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Derives an empty `ClientCliOverrides` companion struct and a no-op `apply_to` impl.
///
/// Field-level `#[client_cli(...)]` attributes will be handled in a later commit.
#[proc_macro_derive(ClientCliArgs, attributes(client_cli))]
pub fn derive_client_cli_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let conf_name = &input.ident;

    let overrides_name = syn::Ident::new("ClientCliOverrides", conf_name.span());

    let expanded = quote! {
        #input

        #[derive(Debug, Default, Clone, PartialEq, Eq)]
        pub struct #overrides_name {}

        impl #overrides_name {
            /// Applies CLI overrides onto the target configuration struct.
            pub fn apply_to(&self, _target: &mut #conf_name) -> Result<(), String> {
                Ok(())
            }
        }
    };

    expanded.into()
}
