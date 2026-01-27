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

use clap::{Parser, Subcommand};
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use curvine_s3_gateway::auth::{
    AccessKeyStoreEnum, CredentialEntry, CredentialStore, CurvineAccessKeyStore,
    LocalAccessKeyStore,
};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use rand::Rng;
use serde::Serialize;
use serde_json::json;
use serde_with::skip_serializing_none;
use std::sync::Arc;

#[skip_serializing_none]
#[derive(Debug, Parser, Clone, Serialize)]
#[command(version = version::VERSION)]
pub struct ObjectArgs {
    #[arg(
        short,
        long,
        help = "Configuration file path (optional)",
        default_value = "etc/curvine-cluster.toml"
    )]
    pub conf: String,

    #[arg(long, default_value = "0.0.0.0:9900")]
    pub listen: String,

    #[arg(long, default_value = "us-east-1")]
    pub region: String,

    #[arg(long)]
    pub credentials_path: Option<String>,

    #[serde(skip_serializing)]
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
        #[arg(long)]
        secret_key: String,
        #[arg(long)]
        description: Option<String>,
    },
    Generate {
        #[arg(long)]
        description: Option<String>,
    },
    List {
        #[arg(long)]
        show_secrets: bool,
    },
    Stats,
}

fn main() -> CommonResult<()> {
    let args = ObjectArgs::parse();

    let args_value = serde_json::to_value(&args).unwrap();
    let args_json = json!({
        "s3_gateway": args_value,
    })
    .to_string();
    let conf = ClusterConf::from(&args.conf, Some(&args_json))?;
    println!("Loaded configuration from {}", &args.conf);

    orpc::common::Logger::init(conf.master.log.clone());

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
            add_generated(store, entry, &access_key, &secret_key).await?
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
        return orpc::err_box!("Invalid access key length. Should be 16-32 characters.");
    }
    Ok(())
}

fn validate_secret_key(secret_key: &str) -> Result<(), String> {
    if secret_key.len() < 16 || secret_key.len() > 64 {
        return orpc::err_box!("Invalid secret key length. Should be 16-64 characters.");
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

async fn add_generated(
    store: &(impl CredentialStore + ?Sized),
    entry: CredentialEntry,
    access_key: &str,
    secret_key: &str,
) -> CommonResult<()> {
    match store.add_credential(entry).await {
        Ok(()) => {
            println!("✓ Successfully generated and added new credential:");
            println!("  Access Key: {}", access_key);
            println!("  Secret Key: {}", secret_key);
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
                    println!("  Secret Key: {}", entry.secret_key.expose());
                } else {
                    println!("  Secret Key: [HIDDEN - use --show-secrets to reveal]");
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
