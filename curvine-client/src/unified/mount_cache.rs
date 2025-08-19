use crate::unified::{UfsFileSystem, UnifiedFileSystem};
use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::FsResult;
use orpc::sync::FastSyncCache;
use orpc::CommonResult;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct MountValue {
    pub info: Arc<MountInfo>,
    pub ufs: UfsFileSystem,
}

impl MountValue {
    pub fn new(info: MountInfo) -> FsResult<Self> {
        let ufs_path = Path::from_str(&info.ufs_path)?;
        let ufs = UfsFileSystem::new(&ufs_path, info.properties.clone())?;

        Ok(Self {
            info: Arc::new(info),
            ufs,
        })
    }

    // Get the ufs path of the cv path
    pub fn get_ufs_path(&self, path: &Path) -> CommonResult<Path> {
        // parse real path
        // mnt: /xuen-test s3://flink/xuen-test
        // Path /xuen-test/a -> s3://flink/xuen-test/a
        let sub_path = path.path().replacen(&self.info.cv_path, "", 1);
        Path::from_str(format!("{}/{}", self.info.ufs_path, sub_path))
    }
}

pub struct MountCache {
    cache: FastSyncCache<String, Option<MountValue>>,
}

impl MountCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: FastSyncCache::new(100000, ttl),
        }
    }

    pub async fn get_mount(
        &self,
        cv: &UnifiedFileSystem,
        path: &Path,
    ) -> FsResult<Option<MountValue>> {
        let key = path.path();
        if let Some(v) = self.cache.get(key) {
            return Ok(v.clone());
        }

        let mnt_info = cv.get_mount_info(path).await?;
        let mnt_value = match mnt_info {
            None => None,
            Some(v) => Some(MountValue::new(v)?),
        };

        for item in path.get_possible_mounts() {
            self.cache.insert(item, None);
        }
        self.cache.insert(key.to_string(), mnt_value.clone());

        Ok(mnt_value)
    }

    pub fn remove(&self, path: &Path) {
        self.cache.remove(path.path());
    }
}
