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

// UnifiedFileSystem dispatches through every enabled backend, so async op futures
// nest deep enough to exceed the default query depth limit.
#![recursion_limit = "256"]

use clap::{Parser, Subcommand};
use curvine_alloc as _;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_s3_gateway::auth::{
    AccessKeyStoreEnum, CredentialEntry, CredentialStore, CurvineAccessKeyStore,
    LocalAccessKeyStore, SecretKey,
};
use curvine_sys::version;
use rand::Rng;
use std::sync::Arc;

#[derive(Debug, Parser, Clone)]
#[command(version = version::VERSION)]
pub struct ObjectArgs {
    #[arg(
        short,
        long,
        help = "Configuration file path (optional)",
        default_value = "etc/curvine-cluster.toml"
    )]
    pub conf: String,

    #[arg(
        long,
        help = "Listen address (host:port); overrides s3_gateway.listen (default 0.0.0.0:9900)"
    )]
    pub listen: Option<String>,

    #[arg(
        long,
        help = "S3 region identifier; overrides s3_gateway.region (default us-east-1)"
    )]
    pub region: Option<String>,

    #[arg(
        long,
        help = "Credential store path; overrides s3_gateway.credentials_path"
    )]
    pub credentials_path: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand, Clone)]
pub enum Commands {
    Serve,
    Credential {
        #[command(subcommand)]
        action: CredentialAction,
    },
}

#[derive(Debug, Subcommand, Clone)]
pub enum CredentialAction {
    Add {
        #[arg(long)]
        access_key: String,
        /// Secret key. Prefer CURVINE_SECRET_KEY so it stays out of the
        /// process list and shell history.
        #[arg(long, env = "CURVINE_SECRET_KEY", hide_env_values = true)]
        secret_key: String,
        #[arg(long)]
        description: Option<String>,
    },
    Generate {
        #[arg(long)]
        description: Option<String>,
    },
    List {
        /// Reveal the full secret keys of stored credentials. This is a
        /// deliberate operator action for recovery/debugging; without it each
        /// key is printed masked.
        #[arg(long)]
        show_secrets: bool,
    },
    Stats,
}

impl ObjectArgs {
    /// Applies the command-line values over the `[s3_gateway]` section. A flag
    /// that is given always wins, also when it equals the built-in default.
    fn apply_to(&self, conf: &mut ClusterConf) {
        if let Some(listen) = &self.listen {
            conf.s3_gateway.listen = listen.clone();
        }
        if let Some(region) = &self.region {
            conf.s3_gateway.region = region.clone();
        }
        if let Some(path) = &self.credentials_path {
            conf.s3_gateway.credentials_path = Some(path.clone());
        }
    }
}

fn main() -> CommonResult<()> {
    let args = ObjectArgs::parse();

    let mut conf = match ClusterConf::from(&args.conf) {
        Ok(c) => {
            tracing::info!("Loaded configuration from {}", args.conf);
            c
        }
        Err(e) => {
            println!(
                "Warning: Failed to load config file '{}': {}. Using default configuration",
                args.conf, e
            );
            ClusterConf::default()
        }
    };
    args.apply_to(&mut conf);

    curvine_runtime::common::Logger::init(conf.master.log.clone());

    if let Some(Commands::Credential { action }) = &args.command {
        return handle_credential_command(&args, &conf, action.clone());
    }

    serve_gateway(conf)
}

fn serve_gateway(conf: ClusterConf) -> CommonResult<()> {
    let listen = conf.s3_gateway.listen.clone();
    let region = conf.s3_gateway.region.clone();

    tracing::info!(
        "S3 Gateway configuration - Listen: {}, Region: {}",
        listen,
        region
    );
    tracing::info!("allocator: {}", curvine_alloc::allocator_type_name());

    let rt = Arc::new(AsyncRuntime::new(
        "curvine-s3-gateway",
        conf.client.io_threads,
        conf.client.worker_threads,
    ));

    rt.block_on(curvine_s3_gateway::start_gateway(
        conf,
        listen,
        region,
        rt.clone(),
    ))
}

fn handle_credential_command(
    _args: &ObjectArgs,
    conf: &ClusterConf,
    action: CredentialAction,
) -> CommonResult<()> {
    let rt = Arc::new(AsyncRuntime::new(
        "curvine-s3-credential-manager",
        conf.client.io_threads,
        conf.client.worker_threads,
    ));

    rt.block_on(async {
        let store = create_credential_store(conf, rt.clone()).await?;

        store.initialize().await.map_err(|e| {
            format!(
                "Failed to initialize {} credential store: {}",
                store.store_type(),
                e
            )
        })?;

        handle_credential_action(action, &store).await
    })
}

async fn create_credential_store(
    conf: &ClusterConf,
    rt: Arc<AsyncRuntime>,
) -> CommonResult<AccessKeyStoreEnum> {
    let use_distributed_auth =
        conf.s3_gateway.enable_distributed_auth || conf.worker.enable_s3_gateway;
    if use_distributed_auth {
        create_distributed_store(conf, rt).await
    } else {
        create_local_store(conf).await
    }
}

async fn create_distributed_store(
    conf: &ClusterConf,
    rt: Arc<AsyncRuntime>,
) -> CommonResult<AccessKeyStoreEnum> {
    use curvine_client::unified::UnifiedFileSystem;

    let ufs = UnifiedFileSystem::with_rt(conf.clone(), rt)?;

    let cache_refresh_interval =
        std::time::Duration::from_secs(conf.s3_gateway.cache_refresh_interval_secs);
    let store = CurvineAccessKeyStore::new(
        ufs,
        conf.s3_gateway.credentials_path.as_deref(),
        cache_refresh_interval,
    )
    .map_err(|e| format!("Failed to create distributed credential store: {}", e))?;

    Ok(AccessKeyStoreEnum::Curvine(Arc::new(store)))
}

async fn create_local_store(conf: &ClusterConf) -> CommonResult<AccessKeyStoreEnum> {
    let cache_refresh_interval = Some(std::time::Duration::from_secs(
        conf.s3_gateway.cache_refresh_interval_secs,
    ));
    let store = LocalAccessKeyStore::new(
        conf.s3_gateway.credentials_path.as_deref(),
        cache_refresh_interval,
    )
    .map_err(|e| format!("Failed to create local credential store: {}", e))?;

    Ok(AccessKeyStoreEnum::Local(Arc::new(store)))
}

async fn handle_credential_action(
    action: CredentialAction,
    store: &(impl CredentialStore + ?Sized),
) -> CommonResult<()> {
    match action {
        CredentialAction::Add {
            access_key,
            secret_key,
            description,
        } => {
            println!("Adding new credential...");

            if let Err(e) = validate_access_key(&access_key) {
                eprintln!("✗ {}", e);
                std::process::exit(1);
            }

            if let Err(e) = validate_secret_key(&secret_key) {
                eprintln!("✗ {}", e);
                std::process::exit(1);
            }

            let entry = CredentialEntry::new(access_key.clone(), secret_key, description);
            add_entry(store, entry, &access_key).await?
        }

        CredentialAction::Generate { description } => {
            println!("Generating new random credential...");
            let (access_key, secret_key) = generate_random_credentials();
            let entry = CredentialEntry::new(access_key.clone(), secret_key.clone(), description);
            let secret = SecretKey::from(secret_key);
            add_generated(store, entry, &access_key, &secret).await?
        }

        CredentialAction::List { show_secrets } => {
            println!("Listing all credentials...");
            list_entries(store, show_secrets).await?
        }

        CredentialAction::Stats => {
            println!("Retrieving cache statistics...");
            show_stats(store).await?
        }
    }
    Ok(())
}

fn validate_access_key(access_key: &str) -> Result<(), String> {
    if access_key.len() < 16 || access_key.len() > 32 {
        return curvine_core_error::err_box!(
            "Invalid access key length. Should be 16-32 characters."
        );
    }
    Ok(())
}

fn validate_secret_key(secret_key: &str) -> Result<(), String> {
    if secret_key.len() < 16 || secret_key.len() > 64 {
        return curvine_core_error::err_box!(
            "Invalid secret key length. Should be 16-64 characters."
        );
    }
    Ok(())
}

async fn add_entry(
    store: &(impl CredentialStore + ?Sized),
    entry: CredentialEntry,
    access_key: &str,
) -> CommonResult<()> {
    match store.add_credential(entry).await {
        Ok(()) => {
            println!(
                "✓ Successfully added credential for access key: {}",
                access_key
            );
            Ok(())
        }
        Err(e) => {
            eprintln!("✗ Failed to add credential: {}", e);
            std::process::exit(1);
        }
    }
}

/// Prints a newly generated credential.
///
/// The secret is emitted exactly once because there is no other way for the
/// operator to obtain it — the store keeps it and later listings only show the
/// masked form. Writing it to stdout (not the log) keeps it out of log files;
/// callers are expected to capture it directly.
async fn add_generated(
    store: &(impl CredentialStore + ?Sized),
    entry: CredentialEntry,
    access_key: &str,
    secret_key: &SecretKey,
) -> CommonResult<()> {
    match store.add_credential(entry).await {
        Ok(()) => {
            let generated_secret = secret_key.expose();
            println!("✓ Successfully generated and added new credential:");
            println!("  Access Key: {access_key}");
            // codeql[rust/cleartext-logging]: one-time operator bootstrap output.
            println!("  Secret Key: {generated_secret}");
            println!("\n⚠️  Please save these credentials securely. The secret key will not be shown again.");
            Ok(())
        }
        Err(e) => {
            eprintln!("✗ Failed to add generated credential: {}", e);
            std::process::exit(1);
        }
    }
}

async fn list_entries(
    store: &(impl CredentialStore + ?Sized),
    show_secrets: bool,
) -> CommonResult<()> {
    match store.list_credentials().await {
        Ok(credentials) => {
            if credentials.is_empty() {
                println!("No credentials found.");
                return Ok(());
            }

            println!("\nFound {} credential(s):", credentials.len());
            println!("{:-<80}", "");

            for (i, entry) in credentials.iter().enumerate() {
                println!("Credential #{}", i + 1);
                println!("  Access Key: {}", entry.access_key);

                if show_secrets {
                    // codeql[rust/cleartext-logging]: explicit operator request
                    // (`--show-secrets`) for credential recovery.
                    println!("  Secret Key: {}", entry.secret_key.expose());
                } else {
                    println!("  Secret Key: {}", entry.secret_key.masked());
                }

                println!(
                    "  Created At: {}",
                    entry.created_at.format("%Y-%m-%d %H:%M:%S UTC")
                );

                println!("  Enabled: {}", if entry.enabled { "Yes" } else { "No" });

                if let Some(desc) = &entry.description {
                    println!("  Description: {}", desc);
                }

                if i < credentials.len() - 1 {
                    println!();
                }
            }

            if !show_secrets {
                println!(
                    "\n⚠️  Secret keys are hidden for security. Use --show-secrets to reveal them."
                );
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("✗ Failed to list credentials: {}", e);
            std::process::exit(1);
        }
    }
}

async fn show_stats(store: &(impl CredentialStore + ?Sized)) -> CommonResult<()> {
    let (cache_size, last_modified, cache_age) = store.get_cache_stats().await;

    println!("\nCredential Store Statistics:");
    println!("{:-<40}", "");
    println!("Cache Size: {} credentials", cache_size);

    if let Some(modified) = last_modified {
        match modified.elapsed() {
            Ok(elapsed) => {
                println!(
                    "File Last Modified: {:.1} seconds ago",
                    elapsed.as_secs_f64()
                );
            }
            Err(_) => {
                println!("File Last Modified: [Future timestamp]");
            }
        }
    } else {
        println!("File Last Modified: [Unknown]");
    }

    println!("Cache Age: {:.1} seconds", cache_age.as_secs_f64());

    match store.refresh_cache().await {
        Ok(()) => {
            let (new_cache_size, _, _) = store.get_cache_stats().await;
            if new_cache_size != cache_size {
                println!(
                    "Cache Refreshed: {} credentials (was {})",
                    new_cache_size, cache_size
                );
            } else {
                println!("Cache Status: Up to date");
            }
        }
        Err(e) => {
            eprintln!("⚠️  Failed to refresh cache: {}", e);
        }
    }
    Ok(())
}

fn generate_random_credentials() -> (String, String) {
    let mut rng = rand::thread_rng();

    let access_key = format!(
        "AKIA{}",
        (0..16)
            .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
            .collect::<String>()
            .to_uppercase()
    );

    let secret_key: String = (0..40)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .collect();

    (access_key, secret_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conf_with(listen: &str, region: &str, credentials_path: &str) -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.s3_gateway.listen = listen.to_string();
        conf.s3_gateway.region = region.to_string();
        conf.s3_gateway.credentials_path = Some(credentials_path.to_string());
        conf
    }

    // Passing the built-in default values must still override the file; they
    // used to be treated as "not set", and --credentials-path was never read.
    #[test]
    fn given_flags_override_the_config_even_with_default_values() {
        let args = ObjectArgs::parse_from([
            "curvine-s3-gateway",
            "--listen",
            "0.0.0.0:9900",
            "--region",
            "us-east-1",
            "--credentials-path",
            "/etc/curvine/s3-credentials.jsonl",
        ]);
        let mut conf = conf_with("0.0.0.0:9000", "eu-west-1", "/data/creds.jsonl");
        args.apply_to(&mut conf);

        assert_eq!(conf.s3_gateway.listen, "0.0.0.0:9900");
        assert_eq!(conf.s3_gateway.region, "us-east-1");
        assert_eq!(
            conf.s3_gateway.credentials_path.as_deref(),
            Some("/etc/curvine/s3-credentials.jsonl")
        );
    }

    #[test]
    fn omitted_flags_keep_the_config_values() {
        let args = ObjectArgs::parse_from(["curvine-s3-gateway"]);
        let mut conf = conf_with("0.0.0.0:9000", "eu-west-1", "/data/creds.jsonl");
        args.apply_to(&mut conf);

        assert_eq!(conf.s3_gateway.listen, "0.0.0.0:9000");
        assert_eq!(conf.s3_gateway.region, "eu-west-1");
        assert_eq!(
            conf.s3_gateway.credentials_path.as_deref(),
            Some("/data/creds.jsonl")
        );
    }

    #[test]
    fn credentials_path_reaches_the_credential_commands() {
        let args = ObjectArgs::parse_from([
            "curvine-s3-gateway",
            "--credentials-path",
            "/etc/curvine/s3-credentials.jsonl",
            "credential",
            "list",
        ]);
        assert!(matches!(args.command, Some(Commands::Credential { .. })));

        let mut conf = ClusterConf::default();
        args.apply_to(&mut conf);
        assert_eq!(
            conf.s3_gateway.credentials_path.as_deref(),
            Some("/etc/curvine/s3-credentials.jsonl")
        );
    }
}
