//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use log::info;
use orpc::common::Utils;
use orpc::{err_msg, CommonResult};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::process::{Command, Stdio};

pub struct CommonUtils;

impl CommonUtils {
    pub const JOB_ID_PREFIX: &'static str = "job_";
    pub const CURVINE_STATE_FILE: &'static str = "CURVINE_STATE_FILE";

    pub fn create_job_id(source: impl AsRef<str>) -> String {
        format!("{}{}", Self::JOB_ID_PREFIX, Utils::md5(source))
    }

    pub fn reload_param(env: HashMap<String, String>) -> CommonResult<()> {
        let exe_path = std::env::current_exe()
            .map_err(|e| err_msg!("failed to get current executable path: {}", e))?;
        let args: Vec<String> = std::env::args().collect();

        info!(
            "reloading: executing {:?} with args: {:?}",
            exe_path,
            &args[1..]
        );

        let mut cmd = Command::new(&exe_path);
        cmd.args(&args[1..]);
        cmd.stdin(Stdio::inherit());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());

        for (k, v) in env.clone() {
            cmd.env(k, v);
        }

        let _child = cmd
            .spawn()
            .map_err(|e| err_msg!("Failed to spawn new process: {}", e))?;

        info!("reload: new process spawned successfully");
        Ok(())
    }

    pub fn sign_p2p_policy(
        secret: &str,
        version: u64,
        peer_whitelist: &[String],
        tenant_whitelist: &[String],
    ) -> String {
        if secret.trim().is_empty() {
            return String::new();
        }
        let mut hasher = Sha256::new();
        Self::update_signing_segment(&mut hasher, secret);
        Self::update_signing_segment(&mut hasher, &version.to_string());
        Self::update_signing_segment(&mut hasher, &peer_whitelist.len().to_string());
        for peer in peer_whitelist {
            Self::update_signing_segment(&mut hasher, peer);
        }
        Self::update_signing_segment(&mut hasher, &tenant_whitelist.len().to_string());
        for tenant in tenant_whitelist {
            Self::update_signing_segment(&mut hasher, tenant);
        }
        let digest = hasher.finalize();
        digest.iter().map(|v| format!("{:02x}", v)).collect()
    }

    pub fn verify_p2p_policy_signature(
        secret: &str,
        version: u64,
        peer_whitelist: &[String],
        tenant_whitelist: &[String],
        signature: &str,
    ) -> bool {
        if secret.trim().is_empty() {
            return true;
        }
        let signed = Self::sign_p2p_policy(secret, version, peer_whitelist, tenant_whitelist);
        !signed.is_empty() && !signature.is_empty() && signed == signature
    }

    pub fn verify_p2p_policy_signatures(
        secret: &str,
        version: u64,
        peer_whitelist: &[String],
        tenant_whitelist: &[String],
        signatures: &str,
    ) -> bool {
        if secret.trim().is_empty() {
            return true;
        }
        signatures
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .any(|signature| {
                Self::verify_p2p_policy_signature(
                    secret,
                    version,
                    peer_whitelist,
                    tenant_whitelist,
                    signature,
                )
            })
    }

    fn update_signing_segment(hasher: &mut Sha256, value: &str) {
        hasher.update(value.len().to_string().as_bytes());
        hasher.update(b":");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }
}

#[cfg(test)]
mod tests {
    use super::CommonUtils;

    #[test]
    fn p2p_policy_signature_roundtrip() {
        let peers = vec!["peer-a".to_string()];
        let tenants = vec!["tenant-a".to_string(), "tenant-b".to_string()];
        let signature = CommonUtils::sign_p2p_policy("secret", 7, &peers, &tenants);
        assert!(!signature.is_empty());
        assert!(CommonUtils::verify_p2p_policy_signature(
            "secret", 7, &peers, &tenants, &signature
        ));
        assert!(!CommonUtils::verify_p2p_policy_signature(
            "secret", 8, &peers, &tenants, &signature
        ));
    }

    #[test]
    fn p2p_policy_rotation_signatures_roundtrip() {
        let peers = vec!["peer-a".to_string()];
        let tenants = vec!["tenant-a".to_string()];
        let old_signature = CommonUtils::sign_p2p_policy("old-secret", 7, &peers, &tenants);
        let new_signature = CommonUtils::sign_p2p_policy("new-secret", 7, &peers, &tenants);
        let signatures = format!("{},{}", new_signature, old_signature);
        assert!(CommonUtils::verify_p2p_policy_signatures(
            "old-secret",
            7,
            &peers,
            &tenants,
            &signatures
        ));
        assert!(CommonUtils::verify_p2p_policy_signatures(
            "new-secret",
            7,
            &peers,
            &tenants,
            &signatures
        ));
    }
}
