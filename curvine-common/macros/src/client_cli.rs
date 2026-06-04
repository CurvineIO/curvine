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

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, Lit, Type};

struct ContainerConfig {
    prefix: Option<String>,
    strip_suffix: Option<String>,
}

pub fn derive_client_cli_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_client_cli_args_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_client_cli_args_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let conf_name = &input.ident;
    let overrides_name = format_ident!("{}CliOverrides", conf_name);
    let container = container_config(input)?;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    conf_name,
                    "ClientCliArgs can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                conf_name,
                "ClientCliArgs can only be derived for structs",
            ));
        }
    };

    let mut override_fields = Vec::new();
    let mut apply_stmts = Vec::new();

    for field in fields {
        if should_skip_field(field)? {
            continue;
        }
        let ident = field
            .ident
            .as_ref()
            .expect("named field must have an identifier");
        validate_cli_field_type(field)?;
        let long = cli_long_name(field, &container)?;
        let field_is_option = unwrap_option_type(&field.ty).is_some();
        let inner_ty = unwrap_option_type(&field.ty).unwrap_or(&field.ty);
        let ty = quote! { Option<#inner_ty> };

        override_fields.push(quote! {
            #[arg(long = #long)]
            pub #ident: #ty,
        });
        apply_stmts.push(apply_override_stmt(ident, inner_ty, field_is_option)?);
    }

    Ok(quote! {
        #[derive(Debug, Default, Clone, ::clap::Args)]
        pub struct #overrides_name {
            #(#override_fields)*
        }

        impl #overrides_name {
            /// Applies CLI overrides onto the target configuration struct.
            pub fn apply_to(&self, target: &mut #conf_name) -> ::orpc::CommonResult<()> {
                #(#apply_stmts)*
                Ok(())
            }
        }
    })
}

fn container_config(input: &DeriveInput) -> syn::Result<ContainerConfig> {
    let mut prefix = None;
    let mut strip_suffix = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("client_cli") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                prefix = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("strip_suffix") {
                strip_suffix = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else {
                return Err(meta.error(
                    "unsupported container-level client_cli attribute; use prefix or strip_suffix",
                ));
            }
            Ok(())
        })?;
    }
    Ok(ContainerConfig {
        prefix,
        strip_suffix,
    })
}

fn apply_override_stmt(
    ident: &syn::Ident,
    inner_ty: &Type,
    field_is_option: bool,
) -> syn::Result<TokenStream2> {
    let assign = if field_is_option {
        if is_copy_type(inner_ty) {
            quote! {
                if let Some(v) = self.#ident {
                    target.#ident = Some(v);
                }
            }
        } else {
            quote! {
                if let Some(v) = &self.#ident {
                    target.#ident = Some(v.clone());
                }
            }
        }
    } else if is_copy_type(inner_ty) {
        quote! {
            if let Some(v) = self.#ident {
                target.#ident = v;
            }
        }
    } else {
        quote! {
            if let Some(v) = &self.#ident {
                target.#ident = v.clone();
            }
        }
    };
    Ok(assign)
}

fn should_skip_field(field: &Field) -> syn::Result<bool> {
    if has_serde_skip(field) {
        return Ok(true);
    }
    if let Some(attr) = client_cli_attr(field)? {
        if attr.skip {
            return Ok(true);
        }
    }
    Ok(false)
}

struct ClientCliAttr {
    skip: bool,
    long: Option<String>,
}

impl syn::parse::Parse for ClientCliAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut skip = false;
        let mut long = None;
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            if ident == "skip" {
                skip = true;
            } else if ident == "long" {
                input.parse::<syn::Token![=]>()?;
                let lit: Lit = input.parse()?;
                if let Lit::Str(s) = lit {
                    long = Some(s.value());
                } else {
                    return Err(input.error("long must be a string literal"));
                }
            } else {
                return Err(input.error("unknown client_cli attribute"));
            }
            if input.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(ClientCliAttr { skip, long })
    }
}

fn client_cli_attr(field: &Field) -> syn::Result<Option<ClientCliAttr>> {
    let mut found = None;
    for attr in &field.attrs {
        if attr.path().is_ident("client_cli") {
            if found.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "duplicate #[client_cli] attribute on the same field",
                ));
            }
            found = Some(attr.parse_args()?);
        }
    }
    Ok(found)
}

fn cli_long_name(field: &Field, container: &ContainerConfig) -> syn::Result<String> {
    if let Some(attr) = client_cli_attr(field)? {
        if let Some(long) = attr.long {
            return Ok(long);
        }
    }
    let kebab = field_kebab_name(field, container);
    if let Some(prefix) = &container.prefix {
        Ok(format!("{prefix}.{kebab}"))
    } else {
        Ok(kebab)
    }
}

fn field_kebab_name(field: &Field, container: &ContainerConfig) -> String {
    if let Some(alias) = serde_alias(field) {
        return alias.replace('_', "-");
    }
    let name = field.ident.as_ref().expect("named field").to_string();
    let base = if let Some(suffix) = &container.strip_suffix {
        name.strip_suffix(suffix).unwrap_or(name.as_str())
    } else {
        name.as_str()
    };
    base.replace('_', "-")
}

fn has_serde_skip(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("serde") {
            return false;
        }
        let mut skip = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                skip = true;
            }
            Ok(())
        });
        skip
    })
}

fn serde_alias(field: &Field) -> Option<String> {
    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let mut alias = None;
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("alias") {
                let value = meta.value()?.parse::<syn::LitStr>()?;
                alias = Some(value.value());
            }
            Ok(())
        });
        if parsed.is_ok() {
            if let Some(found) = alias {
                return Some(found);
            }
        }
    }
    None
}

fn validate_cli_field_type(field: &Field) -> syn::Result<()> {
    let inner = unwrap_option_type(&field.ty).unwrap_or(&field.ty);
    if is_supported_cli_type(inner) {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            &field.ty,
            "ClientCliArgs supports String, bool, integer, and f64 types only",
        ))
    }
}

fn unwrap_option_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty {
        let seg = tp.path.segments.last()?;
        if seg.ident == "Option" {
            if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                if let syn::GenericArgument::Type(inner) = args.args.first()? {
                    return Some(inner);
                }
            }
        }
    }
    None
}

fn is_copy_type(ty: &Type) -> bool {
    let Type::Path(tp) = ty else {
        return false;
    };
    let Some(seg) = tp.path.segments.last() else {
        return false;
    };
    let name = seg.ident.to_string();
    name != "String"
        && matches!(
            name.as_str(),
            "bool"
                | "usize"
                | "isize"
                | "u8"
                | "u16"
                | "u32"
                | "u64"
                | "i8"
                | "i16"
                | "i32"
                | "i64"
                | "f64"
        )
}

fn is_supported_cli_type(ty: &Type) -> bool {
    let Type::Path(tp) = ty else {
        return false;
    };
    let Some(seg) = tp.path.segments.last() else {
        return false;
    };
    let name = seg.ident.to_string();
    matches!(
        name.as_str(),
        "String"
            | "bool"
            | "usize"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "f64"
    )
}
