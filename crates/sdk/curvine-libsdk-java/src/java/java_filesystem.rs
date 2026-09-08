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

use crate::java::JavaUtils;
use crate::{FilesystemConf, LibFilesystem, LibFsReader, LibFsWriter};
use curvine_core_error::{err_box, try_err};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::proto::{
    FreeResponse, GetJobStatusResponse, GetMountTableResponse, MountOptionsProto, SetAttrOptsProto,
    SubmitJobResponse,
};
use curvine_fs_api::state::{DeleteResult, LoadJobCommand, SetAttrOpts};
use curvine_fs_api::utils::ProtoUtils;
use curvine_sdk_core::blocking_job as job;
use jni::objects::{JByteArray, JString};
use jni::sys::{jarray, jboolean, jint, jstring};
use jni::JNIEnv;
use prost::Message;

pub struct JavaFilesystem {
    inner: LibFilesystem,
}

fn decode_mount_options(bytes: &[u8]) -> FsResult<curvine_fs_api::state::MountOptions> {
    if bytes.is_empty() {
        return err_box!("mount options cannot be empty");
    }
    let options = try_err!(MountOptionsProto::decode(bytes));
    Ok(ProtoUtils::mount_options_from_pb(options))
}

fn decode_set_attr_options(bytes: &[u8]) -> FsResult<SetAttrOpts> {
    if bytes.is_empty() {
        return err_box!("set attr options cannot be empty");
    }
    let options = try_err!(SetAttrOptsProto::decode(bytes));
    Ok(ProtoUtils::set_attr_opts_from_pb(options))
}

fn decode_load_job_command(
    env: &mut JNIEnv,
    source_path: JString,
    target_path: JString,
    overwrite: jboolean,
    replicas: Option<jint>,
) -> FsResult<LoadJobCommand> {
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
    if let Some(replicas) = replicas {
        if replicas <= 0 {
            return err_box!("load replicas must be greater than zero");
        }
        builder = builder.replicas(replicas);
    }
    Ok(builder.build())
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

    pub fn set_attr(
        &self,
        env: &mut JNIEnv,
        path: JString,
        options: JByteArray,
    ) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let options = env
            .convert_byte_array(&options)
            .map_err(FsError::from_error)?;
        let status = self
            .inner
            .set_attr(path, decode_set_attr_options(&options)?)?;
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

    pub fn delete(
        &self,
        env: &mut JNIEnv,
        path: JString,
        recursive: jboolean,
    ) -> FsResult<DeleteResult> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.delete(path, JavaUtils::jbool_to_bool(recursive))
    }

    pub fn free(&self, env: &mut JNIEnv, path: JString, recursive: jboolean) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let result = self.inner.free(path, JavaUtils::jbool_to_bool(recursive))?;
        let response = FreeResponse {
            res: ProtoUtils::free_res_to_pb(result),
        };
        let bytes = ProtoUtils::encode(response)?;
        Ok(JavaUtils::new_jarray(env, &bytes)?)
    }

    pub fn get_filesystem_info(&self, env: &mut JNIEnv) -> FsResult<jarray> {
        let status = self.inner.get_filesystem_info()?;
        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn get_mount_info(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let bytes = self.inner.get_mount_info(path)?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    pub fn mount(
        &self,
        env: &mut JNIEnv,
        ufs_path: JString,
        cv_path: JString,
        options: JByteArray,
    ) -> FsResult<()> {
        let ufs_path = JavaUtils::jstring_to_string(env, &ufs_path)?;
        let cv_path = JavaUtils::jstring_to_string(env, &cv_path)?;
        let options = env
            .convert_byte_array(&options)
            .map_err(FsError::from_error)?;
        self.inner
            .mount(ufs_path, cv_path, decode_mount_options(&options)?)
    }

    pub fn unmount(&self, env: &mut JNIEnv, cv_path: JString) -> FsResult<()> {
        let cv_path = JavaUtils::jstring_to_string(env, &cv_path)?;
        self.inner.umount(cv_path)
    }

    pub fn get_mount_table(&self, env: &mut JNIEnv) -> FsResult<jarray> {
        let response = GetMountTableResponse {
            mount_table: self
                .inner
                .get_mount_table()?
                .into_iter()
                .map(ProtoUtils::mount_info_to_pb)
                .collect(),
        };
        let bytes = ProtoUtils::encode(response)?;
        Ok(JavaUtils::new_jarray(env, &bytes)?)
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
        replicas: Option<jint>,
    ) -> FsResult<jarray> {
        let command = decode_load_job_command(env, source_path, target_path, overwrite, replicas)?;
        let result = job::submit_load_job(self.inner.session(), command)?;
        let response = SubmitJobResponse {
            job_id: result.job_id,
            target_path: result.target_path,
            state: i32::from(result.state as i8),
        };
        let bytes = ProtoUtils::encode(response)?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    /// Submit a Curvine-to-UFS export job. Mirrors Rust `JobClient::submit_export_job`.
    pub fn submit_export_job(
        &self,
        env: &mut JNIEnv,
        source_path: JString,
        overwrite: jboolean,
    ) -> FsResult<jarray> {
        let source = JavaUtils::jstring_to_string(env, &source_path)?;
        let command =
            LoadJobCommand::builder(source).overwrite(JavaUtils::jbool_to_bool(overwrite));
        let result = job::submit_export_job(self.inner.session(), command.build())?;
        let response = SubmitJobResponse {
            job_id: result.job_id,
            target_path: result.target_path,
            state: i32::from(result.state as i8),
        };
        let bytes = ProtoUtils::encode(response)?;
        Ok(JavaUtils::new_jarray(env, &bytes)?)
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

    pub fn retry_job(&self, env: &mut JNIEnv, job_id: JString) -> FsResult<jstring> {
        let job_id = JavaUtils::jstring_to_string(env, &job_id)?;
        let retry_job_id = job::retry_job(self.inner.session(), job_id)?;
        JavaUtils::new_jstring(env, Some(retry_job_id)).map_err(|e| FsError::common(e.to_string()))
    }

    pub fn cleanup(&self) {
        self.inner.cleanup()
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_mount_options, decode_set_attr_options};

    #[test]
    fn rejects_empty_mount_options() {
        let error = decode_mount_options(&[]).unwrap_err();
        assert!(error.to_string().contains("mount options cannot be empty"));
    }

    #[test]
    fn rejects_empty_set_attr_options() {
        let error = decode_set_attr_options(&[]).unwrap_err();
        assert!(error
            .to_string()
            .contains("set attr options cannot be empty"));
    }
}
