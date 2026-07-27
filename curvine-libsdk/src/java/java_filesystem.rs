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

use crate::core::job;
use crate::java::JavaUtils;
use crate::{FilesystemConf, LibFilesystem, LibFsReader, LibFsWriter};
use curvine_common::proto::{GetJobStatusResponse, SubmitJobResponse};
use curvine_common::state::LoadJobCommand;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use jni::objects::JString;
use jni::sys::{jarray, jboolean, jstring};
use jni::JNIEnv;

pub struct JavaFilesystem {
    inner: LibFilesystem,
}

impl JavaFilesystem {
    pub fn new(env: &mut JNIEnv, conf: JString) -> FsResult<Self> {
        let toml_str = JavaUtils::jstring_to_string(env, &conf)?;
        let fs_conf = FilesystemConf::from_str(toml_str)?;
        let cluster_conf = fs_conf.into_cluster_conf()?;

        let inner = LibFilesystem::new(cluster_conf)?;
        Ok(Self { inner })
    }

    pub fn create(
        &self,
        env: &mut JNIEnv,
        path: JString,
        overwrite: jboolean,
    ) -> FsResult<LibFsWriter> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.create(path, JavaUtils::jbool_to_bool(overwrite))
    }

    pub fn append(&self, env: &mut JNIEnv, path: JString) -> FsResult<LibFsWriter> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.append(path)
    }

    pub fn open(&self, env: &mut JNIEnv, path: JString) -> FsResult<LibFsReader> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.open(path)
    }

    pub fn mkdir(
        &self,
        env: &mut JNIEnv,
        path: JString,
        create_parent: jboolean,
    ) -> FsResult<bool> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner
            .mkdir(path, JavaUtils::jbool_to_bool(create_parent))
    }

    pub fn get_file_status(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let status = self.inner.get_status(path)?;

        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn list_status(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let status = self.inner.list_status(path)?;

        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn rename(&self, env: &mut JNIEnv, src: JString, dst: JString) -> FsResult<bool> {
        let src = JavaUtils::jstring_to_string(env, &src)?;
        let dst = JavaUtils::jstring_to_string(env, &dst)?;
        self.inner.rename(src, dst)
    }

    pub fn delete(&self, env: &mut JNIEnv, path: JString, recursive: jboolean) -> FsResult<()> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.delete(path, JavaUtils::jbool_to_bool(recursive))
    }

    pub fn get_master_info(&self, env: &mut JNIEnv) -> FsResult<jarray> {
        let status = self.inner.get_master_info()?;
        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn get_mount_info(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let bytes = self.inner.get_mount_info(path)?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    pub fn toggle_path(
        &self,
        env: &mut JNIEnv,
        path: JString,
        check_cache: jboolean,
    ) -> FsResult<jstring> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let ufs_path = self
            .inner
            .toggle_path(path, JavaUtils::jbool_to_bool(check_cache))?;
        let string = JavaUtils::new_jstring(env, ufs_path.map(|x| x.clone_display_path()))?;
        Ok(string)
    }

    /// Submit a UFS-to-Curvine load job. Mirrors Rust `JobClient::submit_load_job`.
    pub fn submit_load_job(
        &self,
        env: &mut JNIEnv,
        source_path: JString,
        target_path: JString,
        overwrite: jboolean,
    ) -> FsResult<jarray> {
        let source = JavaUtils::jstring_to_string(env, &source_path)?;
        let target = if target_path.is_null() {
            None
        } else {
            let value = JavaUtils::jstring_to_string(env, &target_path)?;
            if value.trim().is_empty() {
                None
            } else {
                Some(value)
            }
        };

        let mut builder =
            LoadJobCommand::builder(source).overwrite(JavaUtils::jbool_to_bool(overwrite));
        if let Some(target) = target {
            builder = builder.target_path(target);
        }
        let result = job::submit_load_job(self.inner.session(), builder.build())?;
        let response = SubmitJobResponse {
            job_id: result.job_id,
            target_path: result.target_path,
            state: i32::from(result.state as i8),
        };
        let bytes = ProtoUtils::encode(response)?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    /// Query load job status by id. Mirrors Rust `JobClient::get_status`.
    pub fn get_job_status(&self, env: &mut JNIEnv, job_id: JString) -> FsResult<jarray> {
        let job_id = JavaUtils::jstring_to_string(env, &job_id)?;
        let status = job::get_job_status(self.inner.session(), job_id)?;
        let response = GetJobStatusResponse {
            job_id: status.job_id,
            state: i32::from(status.state as i8),
            source_path: status.source_path,
            target_path: status.target_path,
            progress: ProtoUtils::work_progress_to_pb(status.progress),
        };
        let bytes = ProtoUtils::encode(response)?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    /// Cancel a load job by id. Mirrors Rust `JobClient::cancel`.
    pub fn cancel_job(&self, env: &mut JNIEnv, job_id: JString) -> FsResult<()> {
        let job_id = JavaUtils::jstring_to_string(env, &job_id)?;
        job::cancel_job(self.inner.session(), job_id)
    }

    pub fn cleanup(&self) {
        self.inner.cleanup()
    }
}
