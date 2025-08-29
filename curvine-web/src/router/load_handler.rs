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

#![allow(unused)]

use axum::extract::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use curvine_client::rpc::JobMasterClient;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use curvine_common::fs::Path;

use crate::router::RouterHandler;

///Loading the processor, responsible for handling REST API requests related to loading external data
pub struct LoadHandler {
    ///Master client instance
    client: Arc<JobMasterClient>,
}

///Submit a load task request
#[derive(Debug, Deserialize)]
struct SubmitLoadRequest {
    ///External file path, for example: s3://my-bucket/test.text
    path: String,
    ///Configuration information, including the configuration required to connect to the external file system
    configs: HashMap<String, String>,
    ///The cache time is: number + unit (s/m/h/d/w), for example: 10d means 10 days
    #[serde(default)]
    ttl: Option<String>,
    ///Whether to force update the configuration
    #[serde(default)]
    update_config: bool,
    ///Whether to process folders recursively (added fields)
    #[serde(default)]
    recursive: bool,
}

///Submit the load task response
#[derive(Debug, Serialize)]
struct SubmitLoadResponse {
    ///Task ID
    job_id: String,
    ///Target path
    target_path: String,
}

///Query the load task status request
#[derive(Debug, Deserialize)]
struct GetLoadStatusRequest {
    ///Task ID
    job_id: String,
    ///Whether to display detailed information
    #[serde(default)]
    verbose: bool,
    ///Output format: TEXT or JSON
    #[serde(default = "default_format")]
    format: String,
    ///Configuration information (optional, used to find tasks for specific configurations)
    #[serde(default)]
    configs: HashMap<String, String>,
}

fn default_format() -> String {
    "TEXT".to_string()
}

///Subtask status information
#[derive(Debug, Serialize)]
struct SubTaskStatus {
    ///Subtask ID
    task_id: String,
    ///File path
    path: String,
    ///Target path
    target_path: String,
    ///Task status
    state: String,
    ///Total size (bytes)
    total_size: Option<i64>,
    ///Loaded size (bytes)
    loaded_size: Option<i64>,
    ///Percentage of progress
    progress: Option<f64>,
}

///Query the load task status response
#[derive(Debug, Serialize)]
struct GetLoadStatusResponse {
    ///Task ID
    job_id: String,
    ///External file path
    path: String,
    ///Target path
    target_path: String,
    ///Task status
    state: String,
    ///Status message
    message: Option<String>,
    ///Total size (bytes)
    total_size: Option<i64>,
    ///Loaded size (bytes)
    loaded_size: Option<i64>,
    ///Percentage of progress
    progress: Option<f64>,
    ///Creation time (millisecond timestamp)
    create_time: i64,
    ///Update time (millisecond timestamp)
    update_time: i64,
    ///Expiry time (millisecond timestamp)
    expire_time: Option<i64>,
    ///Total number of files (for folder recursive loading)
    total_files: Option<i64>,
    ///Number of completed files (for folder recursive loading)
    completed_files: Option<i64>,
    ///List of subtask status (returned only when verbose=true)
    sub_tasks: Option<Vec<SubTaskStatus>>,
}

///Cancel the load task request
#[derive(Debug, Deserialize)]
struct CancelLoadRequest {
    ///Task ID
    job_id: String,
}

///Cancel the task response
#[derive(Debug, Serialize)]
struct CancelLoadResponse {
    ///Cancel successfully
    success: bool,
    ///information
    message: Option<String>,
}

///API Error Response
#[derive(Debug, Serialize)]
struct ErrorResponse {
    ///Error message
    message: String,
}

impl LoadHandler {
    ///Create a new load processor
    pub fn new(master_client: Arc<JobMasterClient>) -> Self {
        Self {
            client: master_client,
        }
    }

    ///Processing requests for submission loading tasks
    async fn submit_load(
        &self,
        Json(payload): Json<SubmitLoadRequest>,
    ) -> Result<impl IntoResponse, AppError> {
        debug!(
            "Received submitted_load request for path: {}, recursive: {}",
            payload.path, payload.recursive
        );

        let path = Path::from_str(&payload.path).unwrap();
        //Submit loading tasks
        let response = self
            .client
            .submit_load(&path, LoadJobOptions::default())
            .await
            .map_err(|e| AppError::internal(format!("Failed to submit load task: {}", e)))?;

        // Extract task ID and target path from response
        let job_id = response.job_id;
        let target_path = response.target_path;

        // Construct the response
        let response = SubmitLoadResponse {
            job_id,
            target_path,
        };

        Ok(Json(response))
    }

    /// Process query load task status request
    async fn get_load_status(
        &self,
        Json(payload): Json<GetLoadStatusRequest>,
    ) -> Result<impl IntoResponse, AppError> {
        info!(
            "Received get_load_status request for job_id: {}, verbose: {}",
            payload.job_id, payload.verbose
        );

        // Query the loading task status
        let response = self
            .client
            .get_load_status(&payload.job_id)
            .await
            .map_err(|e| AppError::internal(format!("Failed to get load status: {}", e)))?;

        // Calculate progress percentage
        let progress = if let Some(metrics) = &response.metrics {
            if let (Some(total), Some(loaded)) = (metrics.total_size, metrics.loaded_size) {
                if total > 0 {
                    Some((loaded as f64 / total as f64) * 100.0)
                } else {
                    Some(0.0)
                }
            } else {
                None
            }
        } else {
            None
        };

        // Extract timestamps from metrics
        let (create_time, update_time, expire_time) = if let Some(metrics) = &response.metrics {
            (
                metrics.create_time.unwrap_or(0),
                metrics.update_time.unwrap_or(0),
                metrics.expire_time,
            )
        } else {
            (0, 0, None)
        };

        // Extract size information from metrics
        let (total_size, loaded_size) = if let Some(metrics) = &response.metrics {
            (metrics.total_size, metrics.loaded_size)
        } else {
            (None, None)
        };

        // Build a response
        let response_data = GetLoadStatusResponse {
            job_id: response.job_id,
            path: response.path,
            target_path: response.target_path,
            state: LoadState::as_str_name(
                &LoadState::from_i32(response.state).unwrap_or(LoadState::Pending),
            )
            .to_string(),
            message: response.message,
            total_size,
            loaded_size,
            progress,
            create_time,
            update_time,
            expire_time,
            total_files: None,
            completed_files: None,
            sub_tasks: None,
        };

        if payload.verbose {
            info!(
                "Load status for {}: state={}, progress={:.2}%",
                payload.job_id,
                response_data.state,
                response_data.progress.unwrap_or(0.0)
            );
        }

        Ok(Json(response_data))
    }

    /// Handle request to cancel the load task
    async fn cancel_load(
        &self,
        Json(payload): Json<CancelLoadRequest>,
    ) -> Result<impl IntoResponse, AppError> {
        debug!(
            "Received cancel_load request for job_id: {}",
            payload.job_id
        );

        // Cancel the loading task
        let response = self
            .client
            .cancel_load(&payload.job_id)
            .await
            .map_err(|e| AppError::internal(format!("Failed to cancel load task: {}", e)))?;

        // Extract the operation result from the response
        let success = response.success;
        let message = response.message.unwrap_or_else(|| {
            if success {
                "Task cancelled successfully".to_string()
            } else {
                "Failed to cancel task, it may be completed or not found".to_string()
            }
        });

        //Construct the response
        let response = CancelLoadResponse {
            success,
            message: Some(message),
        };

        Ok(Json(response))
    }
}

impl RouterHandler for LoadHandler {
    fn router(&self) -> Router {
        // Create independent handler clone for each route
        let submit_handler = self.clone();
        let status_handler = self.clone();
        let cancel_handler = self.clone();

        Router::new()
            .route(
                "/api/v1/master/submit_job/load",
                post(move |payload| {
                    let handler = submit_handler.clone();
                    async move { handler.submit_load(payload).await }
                }),
            )
            .route(
                "/api/v1/master/progress_job/load",
                get(move |payload| {
                    let handler = status_handler.clone();
                    async move { handler.get_load_status(payload).await }
                }),
            )
            .route(
                "/api/v1/master/stop_job/load",
                post(move |payload| {
                    let handler = cancel_handler.clone();
                    async move { handler.cancel_load(payload).await }
                }),
            )
    }
}

impl Clone for LoadHandler {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
        }
    }
}

/// Application error type
#[derive(Debug)]
struct AppError {
    code: StatusCode,
    message: String,
}

impl AppError {
    /// Create an internal server error
    fn internal(message: String) -> Self {
        Self {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }
    }

    /// Create a resource not found error
    fn not_found(message: String) -> Self {
        Self {
            code: StatusCode::NOT_FOUND,
            message,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = Json(ErrorResponse {
            message: self.message,
        });

        (self.code, body).into_response()
    }
}
