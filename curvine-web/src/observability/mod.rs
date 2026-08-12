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

mod api;
mod builtin_provider;
mod collector;
mod dashboard;
mod discovery;
mod master_filter;
mod model;
mod operations;
mod prom_parse;
mod prometheus_provider;
mod provider;
mod scraper;
mod service;
mod tsdb;

#[cfg(test)]
mod http_tests;

pub use api::router;
pub use service::ObservabilityService;
