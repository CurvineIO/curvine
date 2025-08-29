use crate::master::JobContext;
use curvine_common::state::JobTaskProgress;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::sync::FastDashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<FastDashMap<String, JobContext>>,
}

impl Default for JobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl JobStore {
    pub fn new() -> Self {
        JobStore {
            jobs: Arc::new(FastDashMap::default()),
        }
    }

    pub fn update_progress(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let task_id = task_id.as_ref();

        let mut job = if let Some(job) = self.jobs.get_mut(job_id) {
            job
        } else {
            return err_box!("Not fond job {}", job_id);
        };

        job.update_progress(task_id, progress)
    }
}

impl Deref for JobStore {
    type Target = FastDashMap<String, JobContext>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}
