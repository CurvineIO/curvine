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
use k8s_openapi::api::core::v1::{Pod, Volume, VolumeMount};
use std::collections::BTreeMap;

pub trait PodBuilder {
    fn component_name(&self) -> &'static str;
    fn cluster_id(&self) -> &str;
    fn get_labels(&self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), self.cluster_id().to_string());
        labels.insert("component".to_string(), self.component_name().to_string());
        labels.insert("type".to_string(), "curvine-native-kubernetes".to_string());
        labels
    }

    fn get_selector_labels(&self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), self.cluster_id().to_string());
        labels.insert("component".to_string(), self.component_name().to_string());
        labels
    }

    fn build_base_pod(&self) -> Result<Pod>;

    fn build_volumes(&self) -> Result<Vec<Volume>>;

    fn build_volume_mounts(&self) -> Result<Vec<VolumeMount>>;

    fn pod_template_path(&self) -> Option<&str>;

    fn main_container_name(&self) -> &'static str;
}

pub trait LabeledResourceBuilder {
    fn get_labels(&self) -> BTreeMap<String, String>;

    fn get_selector_labels(&self) -> BTreeMap<String, String> {
        self.get_labels()
    }
}
