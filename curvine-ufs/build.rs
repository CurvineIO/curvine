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

use std::env;
use std::path::PathBuf;

fn main() {
    // Only build FFI bindings if oss feature is enabled
    if env::var("CARGO_FEATURE_OSS").is_err() {
        return;
    }

    // Re-run build script when relevant env vars / files change.
    println!("cargo:rerun-if-env-changed=JINDOSDK_HOME");

    let ffi_dir = PathBuf::from("ffi");
    let ffi_cpp = ffi_dir.join("jindosdk_ffi.cpp");
    let ffi_h = ffi_dir.join("jindosdk_ffi.h");
    println!("cargo:rerun-if-changed={}", ffi_cpp.display());
    println!("cargo:rerun-if-changed={}", ffi_h.display());

    if !ffi_cpp.exists() || !ffi_h.exists() {
        println!("cargo:warning=FFI files not found, skipping build");
        return;
    }

    // Single knob: JINDOSDK_HOME (defaults align with scripts/install-jindosdk.sh)
    //
    // Support both layouts:
    // - Installed layout (scripts/install-jindosdk.sh): $JINDOSDK_HOME/{include,lib}/libjindosdk_c.so
    // - Tarball layout (official JindoSDK): $JINDOSDK_HOME/{include,lib/native}/libjindosdk_c.so
    let home = env::var("JINDOSDK_HOME").unwrap_or_else(|_| "/opt/jindosdk".into());
    let home = home.trim_end_matches('/').to_string();
    let include_dir = format!("{}/include", home);

    let lib_dir_candidates = [format!("{}/lib", home), format!("{}/lib/native", home)];
    let lib_dir = lib_dir_candidates
        .into_iter()
        .find(|p| PathBuf::from(p).exists())
        .unwrap_or_else(|| format!("{}/lib", home));

    let lib_name = "jindosdk_c";

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file(&ffi_cpp)
        .include(&ffi_dir)
        .include(include_dir)
        .compile("jindosdk_ffi");

    // Link JindoSDK C API
    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
    println!("cargo:rustc-link-lib=dylib={}", lib_name);
}
