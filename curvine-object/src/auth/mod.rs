//! Authentication Layer
//!
//! This module provides S3-compatible authentication mechanisms

pub mod v2;
pub mod v4;

pub trait AccesskeyStore: Send + Sync {
    fn get<'a>(
        &'a self,
        accesskey: &'a str,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + Sync + std::future::Future<Output = Result<Option<String>, String>>>,
    >;
}

/// Static access key store for S3 authentication
pub struct StaticAccessKeyStore {
    credentials: std::collections::HashMap<String, String>,
}

impl StaticAccessKeyStore {
    /// Create a new access key store with a single key pair
    pub fn with_single_key(access_key: String, secret_key: String) -> Self {
        let mut credentials = std::collections::HashMap::new();
        credentials.insert(access_key, secret_key);
        StaticAccessKeyStore { credentials }
    }

    /// Create a new access key store from environment variables
    pub fn from_env() -> Result<Self, String> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("CURVINE_ACCESS_KEY"))
            .map_err(|_| "Missing AWS_ACCESS_KEY_ID or CURVINE_ACCESS_KEY".to_string())?;

        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("CURVINE_SECRET_KEY"))
            .map_err(|_| "Missing AWS_SECRET_ACCESS_KEY or CURVINE_SECRET_KEY".to_string())?;

        let mut credentials = std::collections::HashMap::new();
        credentials.insert(access_key, secret_key);

        Ok(StaticAccessKeyStore { credentials })
    }

    /// Generate secure random credentials as fallback
    pub fn secure_default() -> Result<Self, String> {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let access_key: String = (0..20)
            .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
            .collect();

        let secret_key: String = (0..40)
            .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
            .collect();

        let mut credentials = std::collections::HashMap::new();
        credentials.insert(access_key, secret_key);

        Ok(StaticAccessKeyStore { credentials })
    }
}

impl AccesskeyStore for StaticAccessKeyStore {
    fn get<'a>(
        &'a self,
        access_key: &'a str,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + Sync + std::future::Future<Output = Result<Option<String>, String>>>,
    > {
        Box::pin(async move { Ok(self.credentials.get(access_key).cloned()) })
    }
}

// pub use v2::*; // Currently unused
pub use v4::*;
