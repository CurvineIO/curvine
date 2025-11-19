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

use k8s_openapi::api::core::v1::{Container, Pod};

#[derive(Debug, Clone)]
pub struct CurvinePod {
    pod_without_main_container: Pod,
    main_container: Container,
}

impl CurvinePod {
    pub fn new(pod: Pod, main_container_name: &str) -> Self {
        let mut pod_without_main = pod.clone();
        let mut main_container = None;
        let mut other_containers = Vec::new();

        if let Some(ref mut spec) = pod_without_main.spec {
            for container in &spec.containers {
                if container.name == main_container_name {
                    main_container = Some(container.clone());
                } else {
                    other_containers.push(container.clone());
                }
            }
            spec.containers = other_containers;
        } else {
            use k8s_openapi::api::core::v1::PodSpec;
            pod_without_main.spec = Some(PodSpec::default());
        }

        let main = main_container.unwrap_or_else(|| Container {
            name: main_container_name.to_string(),
            ..Default::default()
        });

        Self {
            pod_without_main_container: pod_without_main,
            main_container: main,
        }
    }

    pub fn get_pod_without_main_container(&self) -> &Pod {
        &self.pod_without_main_container
    }

    pub fn get_main_container(&self) -> &Container {
        &self.main_container
    }

    pub fn copy(&self) -> Self {
        Self {
            pod_without_main_container: self.pod_without_main_container.clone(),
            main_container: self.main_container.clone(),
        }
    }

    pub fn build_pod(&self) -> Pod {
        let mut pod = self.pod_without_main_container.clone();
        if let Some(ref mut spec) = pod.spec {
            let mut containers = spec.containers.clone();
            containers.push(self.main_container.clone());
            spec.containers = containers;
        }
        pod
    }
}

impl CurvinePod {
    pub fn builder() -> CurvinePodBuilder {
        CurvinePodBuilder::new()
    }

    pub fn builder_from(curvine_pod: &CurvinePod) -> CurvinePodBuilder {
        CurvinePodBuilder::from(curvine_pod)
    }
}
#[derive(Debug)]
pub struct CurvinePodBuilder {
    pod_without_main_container: Option<Pod>,
    main_container: Option<Container>,
}

impl CurvinePodBuilder {
    pub fn new() -> Self {
        use k8s_openapi::api::core::v1::PodSpec;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        Self {
            pod_without_main_container: Some(Pod {
                metadata: ObjectMeta {
                    ..Default::default()
                },
                spec: Some(PodSpec::default()),
                ..Default::default()
            }),
            main_container: Some(Container::default()),
        }
    }

    pub fn from(curvine_pod: &CurvinePod) -> Self {
        Self {
            pod_without_main_container: Some(curvine_pod.pod_without_main_container.clone()),
            main_container: Some(curvine_pod.main_container.clone()),
        }
    }

    pub fn with_pod(mut self, pod: Pod) -> Self {
        self.pod_without_main_container = Some(pod);
        self
    }

    pub fn with_main_container(mut self, main_container: Container) -> Self {
        self.main_container = Some(main_container);
        self
    }

    pub fn build(self) -> CurvinePod {
        use k8s_openapi::api::core::v1::PodSpec;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        CurvinePod {
            pod_without_main_container: self.pod_without_main_container.unwrap_or_else(|| Pod {
                metadata: ObjectMeta {
                    ..Default::default()
                },
                spec: Some(PodSpec::default()),
                ..Default::default()
            }),
            main_container: self.main_container.unwrap_or_default(),
        }
    }
}

impl Default for CurvinePodBuilder {
    fn default() -> Self {
        Self::new()
    }
}
