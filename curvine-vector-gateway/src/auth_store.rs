use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};

const DEFAULT_LOCAL_CREDENTIALS_PATH: &str = "~/.curvine/credentials.jsonl";

#[async_trait]
pub trait AccessKeyStore: Send + Sync {
    async fn get(&self, access_key: &str) -> Result<Option<String>, String>;
}

#[derive(Clone)]
pub enum AccessKeyStoreEnum {
    Local(Arc<LocalAccessKeyStore>),
}

#[async_trait]
impl AccessKeyStore for AccessKeyStoreEnum {
    async fn get(&self, access_key: &str) -> Result<Option<String>, String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.get(access_key).await,
        }
    }
}

impl AccessKeyStoreEnum {
    pub async fn initialize(&self) -> Result<(), String> {
        match self {
            AccessKeyStoreEnum::Local(store) => store.initialize().await,
        }
    }
}

pub struct LocalAccessKeyStore {
    credentials_path: PathBuf,
    cache_refresh_interval: Duration,
    cache: RwLock<HashMap<String, String>>,
    last_refresh: RwLock<Option<Instant>>,
    refresh_lock: Mutex<()>,
}

impl LocalAccessKeyStore {
    pub fn new(
        credentials_path: Option<&str>,
        cache_refresh_interval: Option<Duration>,
    ) -> Result<Self, String> {
        let path = expand_home(credentials_path.unwrap_or(DEFAULT_LOCAL_CREDENTIALS_PATH))?;
        let interval = cache_refresh_interval.unwrap_or(Duration::from_secs(30));
        Ok(Self {
            credentials_path: path,
            cache_refresh_interval: interval,
            cache: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(None),
            refresh_lock: Mutex::new(()),
        })
    }

    pub async fn initialize(&self) -> Result<(), String> {
        self.refresh_cache(true).await
    }

    async fn refresh_cache(&self, force: bool) -> Result<(), String> {
        if !force && !self.should_refresh() {
            return Ok(());
        }
        let _guard = self.refresh_lock.lock().await;
        if !force && !self.should_refresh() {
            return Ok(());
        }
        let entries = read_credentials(&self.credentials_path).await?;
        let mut cache = self.cache.write().await;
        cache.clear();
        cache.extend(entries);
        *self.last_refresh.write().await = Some(Instant::now());
        Ok(())
    }

    fn should_refresh(&self) -> bool {
        self.last_refresh
            .try_read()
            .ok()
            .and_then(|v| *v)
            .map(|t| t.elapsed() >= self.cache_refresh_interval)
            .unwrap_or(true)
    }
}

#[async_trait]
impl AccessKeyStore for LocalAccessKeyStore {
    async fn get(&self, access_key: &str) -> Result<Option<String>, String> {
        self.refresh_cache(false).await?;
        if let Some(secret) = self.cache.read().await.get(access_key).cloned() {
            return Ok(Some(secret));
        }
        Ok(self.cache.read().await.get(access_key).cloned())
    }
}

#[derive(Deserialize)]
struct CredentialLine {
    access_key: String,
    secret_key: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

fn default_enabled() -> bool {
    true
}

async fn read_credentials(path: &Path) -> Result<HashMap<String, String>, String> {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(format!("read credentials failed: {e}")),
    };

    let mut map = HashMap::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let item: CredentialLine = serde_json::from_str(line)
            .map_err(|e| format!("invalid credential json at line {}: {e}", idx + 1))?;
        if item.enabled {
            map.insert(item.access_key, item.secret_key);
        }
    }
    Ok(map)
}

fn expand_home(path: &str) -> Result<PathBuf, String> {
    if !path.starts_with("~/") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var("HOME").map_err(|e| format!("HOME not set: {e}"))?;
    Ok(PathBuf::from(home).join(path.trim_start_matches("~/")))
}
