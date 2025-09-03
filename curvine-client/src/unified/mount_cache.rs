use crate::unified::{UfsFileSystem, UnifiedFileSystem};
use curvine_common::fs::Path;
use curvine_common::state::MountInfo;
use curvine_common::FsResult;
use log::debug;
use orpc::common::{FastHashMap, LocalTime};
use orpc::sync::AtomicCounter;
use orpc::CommonResult;
use std::sync::{Arc, RwLock};

pub struct MountValue {
    pub info: MountInfo,
    pub ufs: UfsFileSystem,
    pub mount_id: String,
}

impl MountValue {
    pub fn new(info: MountInfo) -> FsResult<Self> {
        let ufs_path = Path::from_str(&info.ufs_path)?;
        let ufs = UfsFileSystem::new(&ufs_path, info.properties.clone())?;
        let mount_id = format!("{}", info.mount_id);

        Ok(Self {
            info,
            ufs,
            mount_id,
        })
    }

    // Get the ufs path of the cv path
    pub fn get_ufs_path(&self, cv_path: &Path) -> CommonResult<Path> {
        self.info.get_ufs_path(cv_path)
    }

    pub fn mount_id(&self) -> &str {
        &self.mount_id
    }
}

pub struct MountCache {
    mounts: RwLock<FastHashMap<String, Arc<MountValue>>>,
    update_interval: u64,
    last_update: AtomicCounter,
}

impl MountCache {
    pub fn new(update_interval: u64) -> Self {
        Self {
            mounts: RwLock::new(FastHashMap::new()),
            update_interval,
            last_update: AtomicCounter::new(0),
        }
    }

    fn need_update(&self) -> bool {
        LocalTime::mills() > self.update_interval + self.last_update.get()
    }

    pub async fn check_update(&self, fs: &UnifiedFileSystem, force: bool) -> FsResult<()> {
        if self.need_update() || force {
            let mounts = fs.get_mount_table().await?;

            let mut state = self.mounts.write().unwrap();
            state.clear();

            for item in mounts {
                let value = MountValue::new(item)?;
                state.insert(value.info.cv_path.clone(), Arc::new(value));
            }

            debug!("update mounts {:?}", state.len());
            self.last_update.set(LocalTime::mills());
        }

        Ok(())
    }

    pub async fn get_mount(
        &self,
        fs: &UnifiedFileSystem,
        path: &Path,
    ) -> FsResult<Option<Arc<MountValue>>> {
        self.check_update(fs, false).await?;

        let state = self.mounts.read().unwrap();
        if state.is_empty() {
            return Ok(None);
        }

        for mount_path in path.get_possible_mounts() {
            if let Some(mount) = state.get(&mount_path).cloned() {
                return Ok(Some(mount));
            }
        }

        Ok(None)
    }

    pub fn remove(&self, path: &Path) {
        let mut state = self.mounts.write().unwrap();
        state.remove(path.path());
    }
}
