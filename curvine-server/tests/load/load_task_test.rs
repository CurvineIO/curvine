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

/*use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use curvine_proto::{LoadState, LoadTaskRequest, LoadTasResponse, LoadTaskReportRequest, LoadMetrics, LoadStateProto};
use curvine_server::master::load::{LoadJob, LoadJobState, LoadManager, MasterFsInterface};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::worker::load::{LoadTask, LoadTaskState, WorkerLoadHandler};
use curvine_ufs_api::fs::curvine_uri::CurvineURI;

// Simulate the Master file system interface
struct MockMasterFsInterface;

impl MasterFsInterface for MockMasterFsInterface {
    //Implement the necessary methods...
}

#[tokio::test]
async fn test_recursive_load_and_status_tracking() {
    //Create LoadManager
    let fs_interface = Arc::new(MockMasterFsInterface {});
    let load_manager = LoadManager::new(fs_interface);
    
    //Submit task
let source_path = "s3://test-bucket/test-folder/";
    let target_path = "/test-bucket/test-folder/";
    let config = r#"{"s3.region":"us-west-2", "s3.endpoint_url":"http://localhost:9000"}"#;
    let recursive = true;
    
    //Simulation tasks
    let job_id = Uuid::new_v4().to_string();
    let mut job = LoadJob::new(
        source_path.to_string(),
        target_path.to_string(),
        config.to_string(),
        1024 *1024, //1MB block size
        Recursive,
    );
    job.job_id = job_id.clone();
    
    //Simulate file list
    let file_paths = vec![
        "s3://test-bucket/test-folder/file1.txt",
        "s3://test-bucket/test-folder/file2.txt",
        "s3://test-bucket/test-folder/file3.txt",
        "s3://test-bucket/test-folder/subfolder/file4.txt",
        "s3://test-bucket/test-folder/subfolder/file5.txt",
    ];
    
    let local_paths = vec![
        "/test-bucket/test-folder/file1.txt",
        "/test-bucket/test-folder/file2.txt",
"/test-bucket/test-folder/file3.txt",
        "/test-bucket/test-folder/subfolder/file4.txt",
        "/test-bucket/test-folder/subfolder/file5.txt",
    ];
    
    //Create subtasks for each file
    for i in 0..file_paths.len() {
        let task_id = Uuid::new_v4().to_string();
        job.sub_tasks.push(task_id);
    }
    
    //Set the total task size
    job.set_total_size(file_paths.len() as u64);
    
    //Add tasks to the manager
    load_manager.add_job(job.clone());
    
    //Simulate the status of the Worker reporting
      for i in 0..file_paths.len() {
        let task_id = job.sub_tasks[i].clone();
        let file_path = file_paths[i];
        let local_path = local_paths[i];
        
        //Simulate file size
        let total_size = 100 *1024 *1024; //100MB
        
        //Create metric information
        let metrics = LoadMetrics {
            job_id: job_id.clone(),
            task_id: task_id.clone(),
            path: file_path.to_string(),
            target_path: local_path.to_string(),
total_size: Some(total_size),
            loaded_size: Some(0),
            create_time: Some(chrono::Utc::now().timestamp_millis()),
            update_time: Some(chrono::Utc::now().timestamp_millis()),
            expire_time: None,
        };
        
        //Send status report: Start loading
        let report = LoadTaskReportRequest {
            job_id: job_id.clone(),
            state: LoadStateProto::LoadStateLoading,
            worker_id: 1,
            metrics: Some(metrics.clone()),
message: Some("Loading in progress".to_string()),
        };
        
        //Process the report
        load_manager.handle_task_report(report).await.unwrap();
        
        //Wait for a short time
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    //Check the task status
    let job_status = load_manager.get_load_job_status(job_id.clone()).await.unwrap().unwrap();
    assert_eq!(job_status.state, LoadJobState::Loading);
    assert_eq!(job_status.sub_tasks.len(), file_paths.len());
assert_eq!(job_status.total_size, file_paths.len() as u64);
    assert_eq!(job_status.loaded_size, 0);
    
    //Simulate some files
    for i in 0..3 {
        let task_id = job.sub_tasks[i].clone();
        let file_path = file_paths[i];
        let local_path = local_paths[i];
        
        //Simulate file size
        let total_size = 100 *1024 *1024; //100MB
        
        //Create metric information
        let metrics = LoadMetrics {
            job_id: job_id.clone(),
            task_id: task_id.clone(),
path: file_path.to_string(),
            target_path: local_path.to_string(),
            total_size: Some(total_size),
            loaded_size: Some(total_size), //All contents have been loaded
            create_time: Some(chrono::Utc::now().timestamp_millis()),
            update_time: Some(chrono::Utc::now().timestamp_millis()),
            expire_time: None,
        };
        
        //Send status report: Complete loading
        let report = LoadTaskReportRequest {
            job_id: job_id.clone(),
state: LoadStateProto::LoadStateCompleted,
            worker_id: 1,
            metrics: Some(metrics.clone()),
            message: Some("Load completed".to_string()),
        };
        
        //Process the report
        load_manager.handle_task_report(report).await.unwrap();
        
        //Wait for a short time
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Check the task status again
    let job_status = load_manager.get_load_job_status(job_id.clone()).await.unwrap().unwrap();
assert_eq!(job_status.state, LoadJobState::Loading); //Still loading because some files are not completed
    assert_eq!(job_status.loaded_size, 3); //3 files have been completed
    
    // Simulate the remaining files
    for i in 3..file_paths.len() {
        let task_id = job.sub_tasks[i].clone();
        let file_path = file_paths[i];
        let local_path = local_paths[i];
        
        // Simulate file size
        let total_size = 100 *1024 *1024; //100MB
        
        // Create metric information
        let metrics = LoadMetrics {
            job_id: job_id.clone(),
task_id: task_id.clone(),
            path: file_path.to_string(),
            target_path: local_path.to_string(),
            total_size: Some(total_size),
            loaded_size: Some(total_size), // All contents have been loaded
            create_time: Some(chrono::Utc::now().timestamp_millis()),
            update_time: Some(chrono::Utc::now().timestamp_millis()),
            expire_time: None,
        };
        
        // Send status report: Complete loading
        let report = LoadTaskReportRequest {
job_id: job_id.clone(),
            state: LoadStateProto::LoadStateCompleted,
            worker_id: 1,
            metrics: Some(metrics.clone()),
            message: Some("Load completed".to_string()),
        };
        
        // Process the report
        load_manager.handle_task_report(report).await.unwrap();
        
        // Wait for a short time
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Final check of task status
let job_status = load_manager.get_load_job_status(job_id.clone()).await.unwrap().unwrap();
    assert_eq!(job_status.state, LoadJobState::Completed); //All files have been completed
    assert_eq!(job_status.loaded_size, file_paths.len() as u64); //All files have been completed
    
    // Verification progress percentage
    let progress = job_status.progress_percentage();
    assert_eq!(progress, 100.0);
}

#[tokio::test]
async fn test_worker_load_handler() {
    // Create a load task request
    let request = LoadTaskRequest {
        job_id: Uuid::new_v4().to_string(),
        source_path: "s3://test-bucket/test.txt".to_string(),
        target_path: "/test-bucket/test.txt".to_string(),
        configs: {
            let mut configs = HashMap::new();
            configs.insert("s3.region".to_string(), "us-west-2".to_string());
            configs.insert("s3.endpoint_url".to_string(), "http://localhost:9000".to_string());
            configs
        },
        ttl: None,
    };
    
    // TODO: Implement a more complete WorkerLoadHandler test
    // Since WorkerLoadHandler relies on the actual FsClient and BlockStore,
    // Here we only verify the basic request processing logic

    //Verify the requested field
    assert_eq!(request.source_path, "s3://test-bucket/test.txt");
    assert_eq!(request.target_path, "/test-bucket/test.txt");
    assert!(request.configs.contains_key("s3.region"));
    assert!(request.configs.contains_key("s3.endpoint_url"));

    // Create a simulated response
    let response = LoadTasResponse {
        job_id: request.job_id.clone(),
        task_id: Uuid::new_v4().to_string(),
target_path: request.target_path.clone(),
        message: Some("Task queued successfully".to_string()),
    };

    // field for verification response
    assert_eq!(response.job_id, request.job_id);
    assert_eq!(response.target_path, request.target_path);
    assert_eq!(response.message, Some("Task queued successfully".to_string()));
} */