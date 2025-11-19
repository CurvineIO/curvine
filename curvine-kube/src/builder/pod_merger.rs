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

use crate::error::Result;
use crate::pod_template::CurvinePod;
use k8s_openapi::api::core::v1::{Pod, Volume, VolumeMount};
use std::collections::{BTreeMap, HashSet};

pub fn merge_pod_with_template(
    template_pod: Option<CurvinePod>,
    builder_pod: Pod,
    builder_volumes: Vec<Volume>,
    builder_mounts: Vec<VolumeMount>,
    builder_labels: BTreeMap<String, String>,
) -> Result<Pod> {
    if let Some(template) = template_pod {
        merge_with_template_impl(
            template,
            builder_pod,
            builder_volumes,
            builder_mounts,
            builder_labels,
        )
    } else {
        apply_builder_to_pod(builder_pod, builder_volumes, builder_mounts)
    }
}

fn merge_with_template_impl(
    template: CurvinePod,
    builder_pod: Pod,
    builder_volumes: Vec<Volume>,
    builder_mounts: Vec<VolumeMount>,
    builder_labels: BTreeMap<String, String>,
) -> Result<Pod> {
    let mut template_pod_spec = template.get_pod_without_main_container().clone();
    let mut template_main_container = template.get_main_container().clone();

    let mut template_metadata = std::mem::take(&mut template_pod_spec.metadata);
    if let Some(ref mut labels) = template_metadata.labels {
        for (k, v) in builder_labels {
            labels.insert(k, v);
        }
    } else {
        template_metadata.labels = Some(builder_labels);
    }
    template_pod_spec.metadata = template_metadata;

    let mut merged_volumes = template_pod_spec
        .spec
        .as_ref()
        .and_then(|s| s.volumes.clone())
        .unwrap_or_default();

    let existing_volume_names: HashSet<String> =
        merged_volumes.iter().map(|v| v.name.clone()).collect();

    for builder_volume in builder_volumes {
        if !existing_volume_names.contains(&builder_volume.name) {
            merged_volumes.push(builder_volume);
        }
    }

    let mut merged_mounts = template_main_container
        .volume_mounts
        .clone()
        .unwrap_or_default();

    validate_volume_mounts(&merged_mounts, &builder_mounts)?;

    let existing_mount_paths: HashSet<String> =
        merged_mounts.iter().map(|m| m.mount_path.clone()).collect();

    for builder_mount in builder_mounts {
        if !existing_mount_paths.contains(&builder_mount.mount_path) {
            merged_mounts.push(builder_mount);
        }
    }

    let mut merged_env = template_main_container.env.clone().unwrap_or_default();

    let builder_env = builder_pod
        .spec
        .as_ref()
        .and_then(|s| s.containers.first())
        .and_then(|c| c.env.clone())
        .unwrap_or_default();

    let existing_env_names: HashSet<String> = merged_env.iter().map(|e| e.name.clone()).collect();

    for builder_env_var in builder_env {
        if existing_env_names.contains(&builder_env_var.name) {
            if let Some(existing) = merged_env
                .iter_mut()
                .find(|e| e.name == builder_env_var.name)
            {
                *existing = builder_env_var;
            }
        } else {
            merged_env.push(builder_env_var);
        }
    }

    template_main_container.env = Some(merged_env);
    template_main_container.volume_mounts = Some(merged_mounts);

    let builder_container = builder_pod.spec.as_ref().and_then(|s| s.containers.first());

    if template_main_container.working_dir.is_none() {
        template_main_container.working_dir = builder_container.and_then(|c| c.working_dir.clone());
    }

    if template_main_container.args.is_none() {
        template_main_container.args = builder_container.and_then(|c| c.args.clone());
    }

    if template_main_container.resources.is_none()
        || (template_main_container
            .resources
            .as_ref()
            .is_some_and(|r| r.requests.is_none() && r.limits.is_none()))
    {
        template_main_container.resources = builder_container.and_then(|c| c.resources.clone());
    }

    if template_main_container.liveness_probe.is_none() {
        template_main_container.liveness_probe =
            builder_container.and_then(|c| c.liveness_probe.clone());
    }

    if template_main_container.readiness_probe.is_none() {
        template_main_container.readiness_probe =
            builder_container.and_then(|c| c.readiness_probe.clone());
    }

    if template_main_container.lifecycle.is_none() {
        template_main_container.lifecycle = builder_container.and_then(|c| c.lifecycle.clone());
    }

    let mut final_pod = template_pod_spec;
    if let Some(ref mut spec) = final_pod.spec {
        spec.volumes = Some(merged_volumes);
        let mut containers = spec.containers.clone();
        containers.push(template_main_container);
        spec.containers = containers;
    }

    Ok(final_pod)
}

fn apply_builder_to_pod(
    mut pod: Pod,
    volumes: Vec<Volume>,
    mounts: Vec<VolumeMount>,
) -> Result<Pod> {
    if let Some(ref mut spec) = pod.spec {
        spec.volumes = Some(volumes);
        if let Some(ref mut container) = spec.containers.first_mut() {
            container.volume_mounts = Some(mounts);
        }
    }
    Ok(pod)
}

fn validate_volume_mounts(
    user_mounts: &[VolumeMount],
    builder_mounts: &[VolumeMount],
) -> Result<()> {
    use crate::error::KubeError;

    let builder_mount_map: std::collections::HashMap<String, String> = builder_mounts
        .iter()
        .map(|m| (m.name.clone(), m.mount_path.clone()))
        .collect();

    for user_mount in user_mounts {
        if let Some(expected_path) = builder_mount_map.get(&user_mount.name) {
            if &user_mount.mount_path != expected_path {
                return Err(KubeError::ValidationError(format!(
                    "\n Volume mount path mismatch in Pod template\n\
                    \n  Volume Name: '{}'\n\
                    Pod Template mountPath: '{}'\n\
                    Required mountPath: '{}'\n\
                    \n This volume mount path is determined by your curvine-cluster.toml configuration.\n\
                    \n To fix this issue, you have two options:\n\
                    \n  Option 1: Update your Pod template to use the correct mountPath:\n\
                    \n    volumeMounts:\n\
                    - name: {}\n\
                      mountPath: {}  # Must match configuration\n\
                    \n  Option 2: Update your curvine-cluster.toml to match your Pod template:\n\
                    \n    [master]\n\
                    meta_dir = \"{}\"  # Or appropriate config path\n\
                    \n  The mountPath must match the directory paths in curvine-cluster.toml:\n\
                    - master.meta_dir\n\
                    - journal.journal_dir\n\
                    - worker.data_dir (without [DISK] prefix)\n",
                    user_mount.name,
                    user_mount.mount_path,
                    expected_path,
                    user_mount.name,
                    expected_path,
                    user_mount.mount_path
                )));
            }
        }
    }

    Ok(())
}
