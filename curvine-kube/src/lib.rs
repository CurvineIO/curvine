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

pub mod builder;
pub mod client;
pub mod config;
pub mod constants;
pub mod descriptor;
pub mod dynamic_config;
pub mod error;
pub mod pod_template;
pub mod pod_template_utils;
pub mod validator;

pub use config::{
    KubernetesConfig, MasterConfig, ServiceConfig, ServiceType, StorageConfig, WorkerConfig,
};
pub use descriptor::{ClusterInfo, CurvineClusterDescriptor};
pub use error::KubeError;

#[doc(hidden)]
pub use builder::{
    ConfigMapBuilder, HeadlessServiceBuilder, MasterBuilder, ServiceBuilder, WorkerBuilder,
};
#[doc(hidden)]
pub use config::KubernetesConfigBuilder;
