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

use std::sync::Arc;

use curvine_config::{ClusterConf, ConfigSourceMap};
use curvine_runtime::runtime::Runtime;

use crate::router::AdminRouterHandler;
use crate::server::WebHandlerService;

#[derive(Clone)]
pub struct AdminWebService {
    conf: ClusterConf,
    config_sources: ConfigSourceMap,
    rt: Arc<Runtime>,
}

impl AdminWebService {
    pub fn new(conf: ClusterConf, rt: Arc<Runtime>) -> Self {
        Self::with_sources(conf, ConfigSourceMap::new(), rt)
    }

    pub fn with_sources(
        conf: ClusterConf,
        config_sources: ConfigSourceMap,
        rt: Arc<Runtime>,
    ) -> Self {
        Self {
            conf,
            config_sources,
            rt,
        }
    }
}

impl WebHandlerService for AdminWebService {
    type Item = AdminRouterHandler;

    fn get_handler(&self) -> Self::Item {
        AdminRouterHandler::with_rt_and_sources(
            self.conf.clone(),
            self.config_sources.clone(),
            self.rt.clone(),
        )
        .expect("failed to initialize standalone web router")
    }
}
