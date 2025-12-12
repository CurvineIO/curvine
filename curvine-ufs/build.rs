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
    // Only build FFI bindings if oss-hdfs feature is enabled
    if env::var("CARGO_FEATURE_OSS_HDFS").is_ok() {
        println!("cargo:rustc-link-lib=static=jindosdk_ffi");

        // Build the C++ wrapper
        let ffi_dir = PathBuf::from("ffi");
        let ffi_cpp = ffi_dir.join("jindosdk_ffi.cpp");
        let ffi_h = ffi_dir.join("jindosdk_ffi.h");

        if ffi_cpp.exists() && ffi_h.exists() {
            let mut build = cc::Build::new();
            build
                .cpp(true)
                .std("c++17")
                .file(&ffi_cpp)
                .include(&ffi_dir)
                .flag("-std=c++17");

            // Add JindoSDK include paths from environment or default paths
            let jindosdk_include = env::var("JINDOSDK_INCLUDE")
                .unwrap_or_else(|_| "/usr/local/include/jindosdk".to_string());
            let jindosdk_include_alt = env::var("JINDOSDK_INCLUDE_ALT")
                .unwrap_or_else(|_| "/opt/jindosdk/include".to_string());

            build.include(&jindosdk_include);
            build.include(&jindosdk_include_alt);

            // Add JindoSDK library paths
            let jindosdk_lib =
                env::var("JINDOSDK_LIB").unwrap_or_else(|_| "/usr/local/lib".to_string());
            let jindosdk_lib_alt =
                env::var("JINDOSDK_LIB_ALT").unwrap_or_else(|_| "/opt/jindosdk/lib".to_string());

            println!("cargo:rustc-link-search=native={}", jindosdk_lib);
            println!("cargo:rustc-link-search=native={}", jindosdk_lib_alt);

            // Add rpath for runtime
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", jindosdk_lib);

            // Compile our wrapper (which uses jindosdk_c symbols)
            build.compile("jindosdk_ffi");

            // Use link group to ensure symbols are resolved
            println!("cargo:rustc-link-arg=-Wl,--start-group");

            // Link JindoSDK library
            let jindosdk_lib_name =
                env::var("JINDOSDK_LIB_NAME").unwrap_or_else(|_| "jindosdk_c".to_string());
            println!("cargo:rustc-link-lib=dylib={}", jindosdk_lib_name);

            println!("cargo:rustc-link-arg=-Wl,--end-group");

            // Rebuild if FFI files change
            println!("cargo:rerun-if-changed={}", ffi_cpp.display());
            println!("cargo:rerun-if-changed={}", ffi_h.display());
        } else {
            println!("cargo:warning=FFI files not found, skipping build");
        }
    }
}
