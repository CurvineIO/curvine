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
    // Only build FFI bindings if OSS-HDFS feature is enabled.
    // Feature: `oss-hdfs` => env var: CARGO_FEATURE_OSS_HDFS
    if env::var("CARGO_FEATURE_OSS_HDFS").is_err() {
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

    // Single knob: JINDOSDK_HOME (defaults to /opt/jindosdk)
    //
    // Supports both directory layouts:
    // - $JINDOSDK_HOME/{include,lib}/libjindosdk_c.so
    // - $JINDOSDK_HOME/{include,lib/native}/libjindosdk_c.so
    let home = env::var("JINDOSDK_HOME").unwrap_or_else(|_| "/opt/jindosdk".into());
    let home = home.trim_end_matches('/').to_string();
    let include_dir = format!("{}/include", home);

    let lib_name = "jindosdk_c";

    // Get the output directory where the static library will be placed
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");

    // Print friendly build info
    eprintln!("[JindoSDK] Configuring OSS-HDFS support");
    eprintln!("[JindoSDK]   JINDOSDK_HOME: {}", home);
    eprintln!("[JindoSDK]   Include path: {}", include_dir);

    // Compile the FFI wrapper
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file(&ffi_cpp)
        .include(&ffi_dir)
        .include(&include_dir)
        .compile("jindosdk_ffi");

    // Explicitly add the output directory to the link search path
    // This ensures dependent crates can find the static library
    println!("cargo:rustc-link-search=native={}", out_dir);

    // Explicitly link the static library
    println!("cargo:rustc-link-lib=static=jindosdk_ffi");

    // Link JindoSDK C API
    // Add both lib and lib/native to search paths for better compatibility
    // The actual .so files are typically in lib/native for JindoSDK distributions
    let lib_base = format!("{}/lib", home);
    let lib_native = format!("{}/lib/native", home);

    // Verify library file exists and print status
    let lib_file = PathBuf::from(&lib_native).join(format!("lib{}.so", lib_name));
    let lib_exists = lib_file.exists();
    if lib_exists {
        eprintln!("[JindoSDK]   Library found: {}", lib_file.display());
    } else {
        eprintln!(
            "[JindoSDK]   Library location: {} (assuming available at runtime)",
            lib_file.display()
        );
    }

    // Prioritize lib/native first
    println!("cargo:rustc-link-search=native={}", lib_native);
    println!("cargo:rustc-link-search=native={}", lib_base);
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_native);
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_base);
    println!("cargo:rustc-link-lib=dylib={}", lib_name);

    eprintln!("[JindoSDK] Configuration completed successfully");
}
