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

//! JVM management for HDFS operations
//!
//! This module provides thread-safe JVM initialization and management for HDFS operations.
//! It supports both embedded JVM configuration and fallback to WebHDFS when JNI is disabled.
//!
//! # Features
//! - `jni`: Enables native JVM integration for HDFS operations (enabled by default)
//!
//! # Architecture
//! - Uses `OnceLock` for thread-safe lazy initialization
//! - Supports custom JVM builders for flexible configuration
//! - Automatically discovers Hadoop libraries from environment variables
//! - Provides memory management and monitoring utilities
//!
//! # Usage
//! ```rust
//! use curvine_common::jni::jvm::{register_jvm, init_jvm_for_hdfs};
//!
//! // Register JVM builder (call once at startup)
//! register_jvm();
//!
//! // Initialize JVM when needed
//! init_jvm_for_hdfs()?;
//! ```
//!
//! # Environment Variables
//! - `CONNECTOR_LIBS_PATH`: Explicit path to Hadoop libraries
//! - `HADOOP_HOME`: Hadoop installation directory
//! - `HADOOP_CONF_DIR`: Hadoop configuration directory (added to classpath)
//! - `JVM_HEAP_SIZE`: Custom JVM heap size (e.g., "2g", "512m")

use anyhow::{Context, Result};
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use tracing::{error, info, warn};

#[cfg(feature = "jni")]
use jni::{AttachGuard, InitArgsBuilder, JNIEnv, JNIVersion, JavaVM};

pub static JVM: JavaVmWrapper = JavaVmWrapper;

#[cfg(feature = "jni")]
static INSTANCE: OnceLock<JavaVM> = OnceLock::new();

// Mutex to protect JVM initialization from concurrent access
#[cfg(feature = "jni")]
static INIT_LOCK: Mutex<()> = Mutex::new(());

// Track initialization attempts to prevent infinite retries
#[cfg(feature = "jni")]
static INIT_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

// Maximum initialization attempts before giving up
#[cfg(feature = "jni")]
const MAX_INIT_ATTEMPTS: u32 = 3;

static JVM_BUILDER: OnceLock<Box<dyn Fn() -> JavaVM + Send + Sync>> = OnceLock::new();

pub struct JavaVmWrapper;

impl JavaVmWrapper {
    /// Register a JVM builder function for lazy initialization
    ///
    /// # Arguments
    /// * `builder` - Function that creates and configures a JavaVM instance
    ///
    /// # Note
    /// If called multiple times, subsequent calls are ignored with a warning
    pub fn register_jvm(&self, builder: Box<dyn Fn() -> JavaVM + Send + Sync>) {
        if JVM_BUILDER.set(builder).is_err() {
            warn!("JVM builder already registered, ignoring subsequent registration");
        }
    }

    /// Get or initialize the JavaVM instance
    ///
    /// Thread-safe: Uses double-checked locking to ensure only one thread initializes JVM.
    /// Other threads will block on the mutex and wait for initialization to complete.
    ///
    /// # Failure Handling
    /// - If initialization fails, other tasks can retry (up to MAX_INIT_ATTEMPTS)
    /// - Handles poisoned mutex (when initialization panics)
    /// - Tracks retry attempts to prevent infinite loops
    ///
    /// # Returns
    /// Reference to the static JavaVM instance
    ///
    /// # Errors
    /// Returns error if:
    /// - JVM builder is not registered
    /// - JVM initialization fails after MAX_INIT_ATTEMPTS
    /// - Mutex is poisoned and cannot be recovered
    #[cfg(feature = "jni")]
    pub fn get_or_init(&self) -> Result<&'static JavaVM> {
        // Fast path: JVM already initialized
        if let Some(jvm) = INSTANCE.get() {
            return Ok(jvm);
        }

        // Check if we've exceeded retry limit
        let attempts = INIT_ATTEMPTS.load(Ordering::Relaxed);
        if attempts >= MAX_INIT_ATTEMPTS {
            return Err(anyhow::anyhow!(
                "JVM initialization failed after {} attempts, giving up",
                attempts
            ));
        }

        // Slow path: need to initialize JVM with lock protection
        // Handle poisoned mutex (previous initialization panicked)
        let _lock = match INIT_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("JVM initialization lock was poisoned (previous panic), recovering");
                poisoned.into_inner()
            }
        };

        // Double-check: another thread may have initialized while we waited for lock
        if let Some(jvm) = INSTANCE.get() {
            return Ok(jvm);
        }

        // Increment attempt counter
        let attempt = INIT_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        info!(
            "Attempting JVM initialization (attempt {}/{})",
            attempt, MAX_INIT_ATTEMPTS
        );

        // Get builder
        let builder = JVM_BUILDER.get().ok_or_else(|| {
            anyhow::anyhow!("JVM builder must be registered via register_jvm() before first use")
        })?;

        // Try to initialize JVM
        info!("Initializing JVM for HDFS operations (thread-safe initialization)");
        let jvm = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| builder())) {
            Ok(jvm) => {
                info!("JVM initialized successfully on attempt {}", attempt);
                jvm
            }
            Err(panic_err) => {
                error!(
                    "JVM initialization panicked on attempt {}: {:?}",
                    attempt, panic_err
                );
                return Err(anyhow::anyhow!(
                    "JVM initialization panicked on attempt {}",
                    attempt
                ));
            }
        };

        // Store the initialized JVM instance
        INSTANCE.set(jvm).map_err(|_| {
            anyhow::anyhow!("Failed to store JVM instance (concurrent initialization conflict)")
        })?;

        Ok(INSTANCE.get().expect("JVM was just initialized"))
    }

    #[cfg(feature = "jni")]
    pub fn get(&self) -> Option<&'static JavaVM> {
        INSTANCE.get()
    }

    /// Stub method for non-JNI builds
    ///
    /// # Returns
    /// Always returns Ok(()) but logs a warning
    #[cfg(not(feature = "jni"))]
    pub fn get_or_init(&self) -> Result<()> {
        warn!("JNI feature not enabled, HDFS native operations not available");
        Ok(())
    }

    /// Stub method for non-JNI builds
    ///
    /// # Returns
    /// Always returns None
    #[cfg(not(feature = "jni"))]
    pub fn get(&self) -> Option<()> {
        None
    }
}

const DEFAULT_MEMORY_PROPORTION: f64 = 0.1;

/// Locate HDFS libraries path from environment variables or default location
///
/// Priority order:
/// 1. CONNECTOR_LIBS_PATH environment variable
/// 2. HADOOP_HOME/share/hadoop (if exists)
/// 3. HADOOP_HOME (fallback)
/// 4. ./libs (default)
///
/// # Returns
/// Path to HDFS libraries directory
///
/// # Errors
/// Returns error if unable to determine executable path for default location
fn locate_hdfs_libs_path() -> Result<PathBuf> {
    // First priority: explicit connector libs path
    if let Ok(libs_path) = env::var("CONNECTOR_LIBS_PATH") {
        let path = PathBuf::from(libs_path);
        info!(path = %path.display(), "Using CONNECTOR_LIBS_PATH");
        return Ok(path);
    }

    // Second priority: HADOOP_HOME with share/hadoop subdirectory
    if let Ok(hadoop_home) = env::var("HADOOP_HOME") {
        let hadoop_path = PathBuf::from(hadoop_home);
        let base_hadoop_dir = hadoop_path.join("share/hadoop");

        if base_hadoop_dir.exists() {
            info!(path = %base_hadoop_dir.display(), "Using HADOOP_HOME/share/hadoop");
            return Ok(base_hadoop_dir);
        } else {
            warn!(
                hadoop_home = %hadoop_path.display(),
                "Hadoop share/hadoop directory not found, using HADOOP_HOME directly"
            );
            return Ok(hadoop_path);
        }
    }

    // Default: ./libs relative to executable
    let default_path = env::current_exe()
        .context("Unable to get executable path")?
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Cannot get parent directory of executable"))?
        .join("libs");

    info!(path = %default_path.display(), "Using default libs path");
    Ok(default_path)
}

/// Get available system memory in bytes
///
/// Reads from /proc/meminfo on Linux systems. Falls back to 8GB if unable to read.
///
/// # Returns
/// Available memory in bytes
fn get_available_memory() -> usize {
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("MemAvailable:") {
                if let Some(kb) = line.split_whitespace().nth(1) {
                    if let Ok(kb_val) = kb.parse::<usize>() {
                        let bytes = kb_val * 1024;
                        info!(
                            available_memory_mb = bytes / 1024 / 1024,
                            "Detected available system memory"
                        );
                        return bytes;
                    }
                }
            }
        }
    }

    const FALLBACK_MEMORY: usize = 8 * 1024 * 1024 * 1024; // 8GB
    warn!("Unable to read system memory, using fallback: 8GB");
    FALLBACK_MEMORY
}

/// Scan a directory for JAR files and return their canonical paths
#[cfg(feature = "jni")]
fn scan_jar_files(dir: &PathBuf) -> Result<Vec<String>> {
    let mut jars = Vec::new();

    if !dir.exists() {
        return Ok(jars);
    }

    let entries =
        std::fs::read_dir(dir).context(format!("Failed to read directory: {}", dir.display()))?;

    for entry in entries.flatten() {
        let entry_path = entry.path();
        if entry_path.extension().map_or(false, |ext| ext == "jar") {
            if let Ok(canonical_path) = std::fs::canonicalize(&entry_path) {
                if let Some(path_str) = canonical_path.to_str() {
                    jars.push(path_str.to_owned());
                }
            }
        }
    }

    info!(dir = %dir.display(), count = jars.len(), "Scanned JAR files");
    Ok(jars)
}

/// Build classpath for HDFS operations
#[cfg(feature = "jni")]
fn build_classpath(libs_path: &PathBuf) -> Result<Vec<String>> {
    let mut class_vec = Vec::new();

    // Add HADOOP_CONF_DIR to classpath if available
    if let Ok(hadoop_conf_dir) = env::var("HADOOP_CONF_DIR") {
        class_vec.push(hadoop_conf_dir);
        info!("Added HADOOP_CONF_DIR to classpath");
    }

    // Define standard Hadoop JAR directories
    let jar_dirs = [
        libs_path.join("common"),
        libs_path.join("common").join("lib"),
        libs_path.join("hdfs"),
        libs_path.join("hdfs").join("lib"),
        libs_path.join("client"),
        libs_path.clone(),
    ];

    // Scan each directory for JAR files
    for jar_dir in jar_dirs.iter() {
        let jars = scan_jar_files(jar_dir)?;
        class_vec.extend(jars);
    }

    info!(total_jars = class_vec.len(), "Built classpath");
    Ok(class_vec)
}

/// Calculate JVM heap size based on environment or system memory
#[cfg(feature = "jni")]
fn calculate_heap_size() -> String {
    env::var("JVM_HEAP_SIZE").unwrap_or_else(|_| {
        let memory_bytes = (get_available_memory() as f64 * DEFAULT_MEMORY_PROPORTION) as usize;
        format!("{}m", memory_bytes / 1024 / 1024)
    })
}

/// Build and configure JavaVM for HDFS operations
///
/// # Returns
/// Configured JavaVM instance ready for HDFS operations
///
/// # Errors
/// Returns error if unable to locate HDFS libraries, build classpath, or create JVM
#[cfg(feature = "jni")]
pub fn build_jvm_for_hdfs() -> Result<JavaVM> {
    let libs_path = locate_hdfs_libs_path().context("Failed to locate HDFS libs")?;
    info!(path = %libs_path.display(), "Located HDFS libs");

    let class_vec = build_classpath(&libs_path).context("Failed to build classpath")?;
    let heap_size = calculate_heap_size();

    info!(heap_size = %heap_size, "Calculated JVM heap size");

    let args_builder = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option("-Dis_embedded_connector=true")
        .option(format!("-Djava.class.path={}", class_vec.join(":")))
        .option("-Xms64m")
        .option(format!("-Xmx{}", heap_size));

    let jvm_args = args_builder.build().context("Invalid JVM arguments")?;

    let jvm = JavaVM::new(jvm_args).context("Failed to create JavaVM")?;

    info!("JavaVM initialized successfully for HDFS operations");
    Ok(jvm)
}

#[cfg(feature = "jni")]
pub fn jvm_env(jvm: &JavaVM) -> Result<AttachGuard<'_>, jni::errors::Error> {
    jvm.attach_current_thread()
        .inspect_err(|e| error!(error = ?e, "JVM attach thread error"))
}

/// Register the JVM builder for HDFS operations
///
/// This function registers a builder that will create and configure a JavaVM
/// when first needed. The builder is called lazily on first JVM access.
///
/// # Panics
/// Panics if called more than once
pub fn register_jvm() {
    #[cfg(feature = "jni")]
    {
        JVM.register_jvm(Box::new(|| {
            build_jvm_for_hdfs().expect("Failed to build JVM for HDFS")
        }));
        info!("JVM builder registered for HDFS operations");
    }
    #[cfg(not(feature = "jni"))]
    {
        warn!("JNI feature not enabled, using WebHDFS only");
    }
}

/// Initialize JVM for HDFS operations
///
/// This is a convenience function that registers the JVM builder and initializes the JVM.
/// Safe to call multiple times - subsequent calls are no-ops.
///
/// # Returns
/// Ok(()) on success
///
/// # Errors
/// Returns error if JVM initialization fails
pub fn init_jvm_for_hdfs() -> Result<()> {
    #[cfg(feature = "jni")]
    {
        register_jvm();
        let _jvm = JVM
            .get_or_init()
            .context("Failed to initialize JVM for HDFS")?;
        info!("JVM initialized successfully for HDFS operations");
    }

    #[cfg(not(feature = "jni"))]
    {
        warn!("JNI feature not enabled, using WebHDFS only");
    }

    Ok(())
}

pub fn is_jvm_available() -> bool {
    #[cfg(feature = "jni")]
    {
        JVM.get().is_some()
    }

    #[cfg(not(feature = "jni"))]
    {
        false
    }
}

pub fn load_jvm_memory_stats() -> (usize, usize) {
    #[cfg(feature = "jni")]
    {
        match JVM.get() {
            Some(jvm) => {
                let result: Result<(usize, usize), anyhow::Error> = (|| {
                    execute_with_jni_env(jvm, |env| {
                        let runtime_instance = crate::call_static_method!(
                            env,
                            {Runtime},
                            {Runtime getRuntime()}
                        )?;

                        let total_memory = crate::call_method!(
                            env,
                            runtime_instance.as_ref(),
                            {long totalMemory()}
                        )?;

                        let free_memory = crate::call_method!(
                            env,
                            runtime_instance.as_ref(),
                            {long freeMemory()}
                        )?;

                        Ok((total_memory as usize, (total_memory - free_memory) as usize))
                    })
                })();

                match result {
                    Ok(ret) => ret,
                    Err(e) => {
                        error!(error = ?e, "Failed to collect JVM stats");
                        (0, 0)
                    }
                }
            }
            _ => (0, 0),
        }
    }

    #[cfg(not(feature = "jni"))]
    {
        (0, 0)
    }
}

#[cfg(feature = "jni")]
pub fn execute_with_jni_env<T>(
    jvm: &JavaVM,
    f: impl FnOnce(&mut JNIEnv<'_>) -> Result<T>,
) -> Result<T> {
    let mut env = jvm
        .attach_current_thread()
        .context("Failed to attach current rust thread to jvm")?;

    // java.lang.Thread.currentThread()
    //     .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());

    let thread = crate::call_static_method!(
        env,
        {Thread},
        {Thread currentThread()}
    )?;

    let system_class_loader = crate::call_static_method!(
        env,
        {ClassLoader},
        {ClassLoader getSystemClassLoader()}
    )?;

    crate::call_method!(
        env,
        thread,
        {void setContextClassLoader(ClassLoader)},
        &system_class_loader
    )?;

    let ret = f(&mut env);

    match env.exception_check() {
        Ok(true) => {
            let exception = env.exception_occurred().inspect_err(|e| {
                warn!(error = ?e, "Failed to get jvm exception");
            })?;
            env.exception_describe().inspect_err(|e| {
                warn!(error = ?e, "Failed to describe jvm exception");
            })?;
            env.exception_clear().inspect_err(|e| {
                warn!(error = ?e, "Exception occurred but failed to clear");
            })?;
            let message = crate::call_method!(env, exception, {String getMessage()})?;
            let message = jobj_to_str(&mut env, message)?;
            return Err(anyhow::anyhow!("Caught Java Exception: {}", message));
        }
        Ok(false) => {}
        Err(e) => {
            warn!(error = ?e, "Failed to check exception");
        }
    }

    ret
}

#[cfg(feature = "jni")]
pub fn jobj_to_str(env: &mut JNIEnv<'_>, obj: jni::objects::JObject<'_>) -> Result<String> {
    use jni::objects::JString;

    if !env.is_instance_of(&obj, "java/lang/String")? {
        anyhow::bail!("Input object is not a java string and can't be converted!")
    }
    let jstr = JString::from(obj);
    let java_str = env.get_string(&jstr)?;
    Ok(java_str.to_str()?.to_owned())
}

///
/// # Returns
///
/// - `Ok(None)` if JVM is not initialized.
/// - `Ok(Some(String))` if JVM is initialized and stack traces are dumped.
/// - `Err` if failed to dump stack traces.
pub fn dump_jvm_stack_traces() -> Result<Option<String>> {
    #[cfg(feature = "jni")]
    {
        match JVM.get() {
            None => Ok(None),
            Some(jvm) => execute_with_jni_env(jvm, |env| {
                let thread_class = env.find_class("java/lang/Thread")?;
                let thread_map = env.call_static_method(
                    thread_class,
                    "getAllStackTraces",
                    "()Ljava/util/Map;",
                    &[],
                )?;

                let map_obj = thread_map.l()?;
                let to_string_result =
                    env.call_method(map_obj, "toString", "()Ljava/lang/String;", &[])?;

                let jstr = jni::objects::JString::from(to_string_result.l()?);
                let java_str = env.get_string(&jstr)?;
                Ok(Some(java_str.to_str()?.to_owned()))
            }),
        }
    }

    #[cfg(not(feature = "jni"))]
    {
        Ok(None)
    }
}
