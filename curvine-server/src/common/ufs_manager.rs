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

use crate::common::UfsClient;
use curvine_client::file::FsClient;
use curvine_common::conf::{UfsConf, UfsConfBuilder};
use curvine_common::fs::CurvineURI;
use curvine_common::proto::MountPointInfo;
use curvine_common::FsResult;
use curvine_ufs::fs::ufs_context::UFSContext;
use log::info;
use orpc::err_box;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A class that manages the UFS for master servers.
/// When loading, directly use the MountTable to return a usable UfsClient to the user
pub struct UfsManager {
    curvine_client: Arc<FsClient>,
    mount_table: RwLock<HashMap<String, MountPointInfo>>, //ufs_path -> mountPointInfo
}

/// manager all ufs associate with mount table
impl UfsManager {
    pub fn new(fs_client: Arc<FsClient>) -> Self {
        UfsManager {
            curvine_client: fs_client.clone(),
            mount_table: RwLock::new(HashMap::new()),
        }
    }

    pub async fn sync_mount_table(&mut self) -> FsResult<()> {
        let resp = self.curvine_client.get_mount_table().await?;
        let mut table = self.mount_table.write().unwrap();
        resp.mount_points.into_iter().for_each(|mnt| {
            let ufs_path = mnt.ufs_path.clone();
            table.insert(ufs_path, mnt);
        });
        Ok(())
    }

    async fn get_ufs_conf(&mut self, ufs_base_uri: &String) -> FsResult<UfsConf> {
        let mount_point_info = self.get_mount_point(ufs_base_uri).await?;
        let mut ufs_conf_builder = UfsConfBuilder::default();
        for (k, v) in &mount_point_info.properties {
            ufs_conf_builder.add_config(k.clone(), v.clone());
        }
        let ufs_conf = ufs_conf_builder.build();
        Ok(ufs_conf)
    }

    async fn get_mount_point(&mut self, ufs_base_uri: &String) -> FsResult<MountPointInfo> {
        // looking cache
        {
            let table = self.mount_table.read().unwrap();
            match table.get(ufs_base_uri) {
                Some(mount_point_info) => {
                    return Ok(mount_point_info.clone());
                }
                None => {
                    info!("not found mount point info from caching, get from master");
                }
            };
        }

        let resp = self.curvine_client.get_mount_point(ufs_base_uri).await?;
        match resp {
            Some(mount_point_info) => {
                let mut table = self.mount_table.write().unwrap();
                let ret = mount_point_info.clone();
                table.insert(ufs_base_uri.clone(), mount_point_info);
                Ok(ret)
            }
            None => err_box!("failed trans ufs_uri to curvine path, maybe mount first"),
        }
    }

    pub async fn get_curvine_path(&mut self, ufs_uri: &CurvineURI) -> FsResult<String> {
        let ufs_norm_uri = match ufs_uri.normalize_uri() {
            Some(ufs_base_uri) => ufs_base_uri,
            None => return err_box!("invalid ufs_uri, can't find baseuri"),
        };

        // "s3://a/b/" -> "s3://a/b"
        let ufs_norm_path = ufs_uri.full_path().trim_end_matches('/');
        let mount_point_info = self.get_mount_point(&ufs_norm_uri).await?;

        let curvine_path = mount_point_info.curvine_path.clone();
        let curvine_path = curvine_path.trim_end_matches('/');

        // Extract the relative path by removing the mount point prefix
        // mount info ufs_path -> "s3://a/b/,        mnt_path -> /x
        //       ufs_norm_path -> "s3://a/b/c ,  curvine_path -> /x/c
        let ufs_path = if ufs_norm_uri.starts_with(&mount_point_info.ufs_path) {
            let prefix_len = mount_point_info.ufs_path.len();
            if prefix_len < ufs_norm_path.len() {
                &ufs_norm_path[prefix_len..]
            } else {
                ""
            }
        } else {
            return err_box!("UFS URI does not start with mount point path");
        };

        Ok(format!("{}{}", curvine_path, ufs_path))
    }

    /// get client with cache
    pub async fn get_client(&mut self, ufs_uri: &CurvineURI) -> FsResult<Arc<UfsClient>> {
        let scheme = ufs_uri.scheme();
        if scheme.is_none() {
            return err_box!("none scheme provided!");
        }

        let ufs_base_uri = match ufs_uri.normalize_uri() {
            Some(ufs_base_uri) => ufs_base_uri,
            None => return err_box!("none baseuri provided!"),
        };

        let ufs_conf = self.get_ufs_conf(&ufs_base_uri).await?;
        let ufs_ctx = Arc::new(UFSContext::new(scheme.unwrap().to_string(), ufs_conf));
        let ufs_client = Arc::new(UfsClient::new(ufs_ctx)?);
        Ok(ufs_client.clone())
    }

    pub async fn get_client_with_conf(
        &mut self,
        ufs_uri: &CurvineURI,
        ufs_conf: UfsConf,
    ) -> FsResult<Arc<UfsClient>> {
        let scheme = ufs_uri.scheme();
        if scheme.is_none() {
            return err_box!("none scheme provided!");
        }

        let ufs_ctx = Arc::new(UFSContext::new(scheme.unwrap().to_string(), ufs_conf));
        let ufs_client = Arc::new(UfsClient::new(ufs_ctx)?);
        Ok(ufs_client.clone())
    }
}
