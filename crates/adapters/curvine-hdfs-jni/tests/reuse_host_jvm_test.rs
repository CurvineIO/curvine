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

//! The HDFS JNI backend must attach to a JVM that already runs in the process (the Java
//! SDK loaded by Spark) instead of failing with "VM already created".
//!
//! This test plays the `java` launcher: it loads libjvm with `RTLD_GLOBAL` and starts the
//! host JVM itself, then checks that `JVM.get_or_init()` returns that same JVM without
//! calling the registered builder. It needs `JAVA_HOME`; without it the test is skipped.

#![cfg(unix)]

use curvine_hdfs_jni::jni::{jvm_env, JVM};
use jni::objects::{JObject, JString, JValue};
use jni::{InitArgsBuilder, JNIVersion, JavaVM};
use std::path::PathBuf;

const MARKER_KEY: &str = "curvine.test.host.jvm";
const MARKER_VALUE: &str = "host";

fn find_libjvm() -> Option<PathBuf> {
    let java_home = PathBuf::from(std::env::var_os("JAVA_HOME")?);
    let name = if cfg!(target_os = "macos") {
        "libjvm.dylib"
    } else {
        "libjvm.so"
    };
    // JDK 9+ layout first, then JDK 8 (`jre/lib/<arch>/server` on Linux).
    let mut candidates = vec![
        java_home.join("lib/server").join(name),
        java_home.join("jre/lib/server").join(name),
    ];
    for arch in ["amd64", "aarch64"] {
        candidates.push(
            java_home
                .join("jre/lib")
                .join(arch)
                .join("server")
                .join(name),
        );
    }
    candidates.into_iter().find(|p| p.exists())
}

#[test]
fn get_or_init_reuses_host_jvm() {
    let Some(libjvm) = find_libjvm() else {
        eprintln!("JAVA_HOME/libjvm not found, skipping");
        return;
    };

    // Like the `java` launcher: libjvm symbols become globally visible.
    let _libjvm = unsafe {
        libloading::os::unix::Library::open(
            Some(&libjvm),
            libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_GLOBAL,
        )
    }
    .expect("load libjvm");

    let args = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option(format!("-D{MARKER_KEY}={MARKER_VALUE}"))
        .build()
        .expect("jvm args");
    let host = JavaVM::with_libjvm(args, || Ok(libjvm.as_os_str())).expect("start host jvm");

    JVM.register_jvm(Box::new(|| {
        Err(anyhow::anyhow!(
            "must reuse the host JVM instead of creating a second one"
        ))
    }));
    let jvm = JVM.get_or_init().expect("reuse host jvm");
    assert_eq!(jvm.get_java_vm_pointer(), host.get_java_vm_pointer());

    // The reused JVM is usable from a Rust thread and sees the host's system properties.
    let mut env = jvm_env(jvm).expect("attach");
    let key: JObject = env.new_string(MARKER_KEY).unwrap().into();
    let value = env
        .call_static_method(
            "java/lang/System",
            "getProperty",
            "(Ljava/lang/String;)Ljava/lang/String;",
            &[JValue::Object(&key)],
        )
        .and_then(|v| v.l())
        .expect("System.getProperty");
    let value: String = env.get_string(&JString::from(value)).unwrap().into();
    assert_eq!(value, MARKER_VALUE);
}
