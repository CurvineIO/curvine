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

/*#[cfg(test)]
mod tests {
    use curvine_common_core::proto::LoadState;
    use curvine_server::master::LoadJob;
    use std::collections::HashMap;

    #[test]
    fn test_update_job_from_sub_tasks() {
        // Create a new loading task
        let mut job = LoadJob::new(
            "source_path".to_string(),
            "target_path".to_string(),
            HashMap::new(),
            1024,
false,
        );

        // Add subtasks
        job.add_sub_task(
            "task1".to_string(),
            "path1".to_string(),
            "target_path1".to_string(),
            1,
        );
        job.add_sub_task(
            "task2".to_string(),
            "path2".to_string(),
            "target_path2".to_string(),
            2,
        );
        job.add_sub_task(
            "task3".to_string(),
            "path3".to_string(),
            "target_path3".to_string(),
3,
        );

        // Test 1: All subtasks have been completed
        job.update_sub_task("task1", LoadState::Completed, Some(100), Some(100), None);
        job.update_sub_task("task2", LoadState::Completed, Some(200), Some(200), None);
        job.update_sub_task("task3", LoadState::Completed, Some(300), Some(300), None);
        job.update_job_from_sub_tasks();
        assert_eq!(job.state, LoadState::Completed);
        assert_eq!(job.total_size, 600);
        assert_eq!(job.loaded_size, 600);

        //Test 2: Some subtasks failed
        job.update_sub_task("task1", LoadState::Failed, Some(100), Some(100), None);
        job.update_sub_task("task2", LoadState::Completed, Some(200), Some(200), None);
        job.update_sub_task("task3", LoadState::Completed, Some(300), Some(300), None);
        job.update_job_from_sub_tasks();
        assert_eq!(job.state, LoadState::Failed);
        assert_eq!(job.total_size, 600);
        assert_eq!(job.loaded_size, 600);

        // Test 3: Some subtasks are cancelled
        job.update_sub_task("task1", LoadState::Canceled, Some(100), Some(100), None);
        job.update_sub_task("task2", LoadState::Completed, Some(200), Some(200), None);
        job.update_sub_task("task3", LoadState::Completed, Some(300), Some(300), None);
        job.update_job_from_sub_tasks();
        assert_eq!(job.state, LoadState::Canceled);
        assert_eq!(job.total_size, 600);
        assert_eq!(job.loaded_size, 600);

        // Test 4: Some subtasks are still loading
        job.update_sub_task("task1", LoadState::Loading, Some(100), Some(100), None);
        job.update_sub_task("task2", LoadState::Completed, Some(200), Some(200), None);
        job.update_sub_task("task3", LoadState::Completed, Some(300), Some(300), None);
        job.update_job_from_sub_tasks();
        assert_eq!(job.state, LoadState::Loading);
        assert_eq!(job.total_size, 600);
        assert_eq!(job.loaded_size, 600);

        // Test 5: Subtask status mixing
        job.update_sub_task("task1", LoadState::Loading, Some(100), Some(100), None);
        job.update_sub_task("task2", LoadState::Failed, Some(200), Some(200), None);
        job.update_sub_task("task3", LoadState::Completed, Some(300), Some(300), None);
        job.update_job_from_sub_tasks();
        assert_eq!(job.state, LoadState::Failed);
        assert_eq!(job.total_size, 600);
        assert_eq!(job.loaded_size, 600);

        // Test 6: Some subtasks are in Pending state
        job.update_sub_task("task1", LoadState::Pending, Some(100), Some(100), None);
        job.update_sub_task("task2", LoadState::Completed, Some(200), Some(200), None);
        job.update_sub_task("task3", LoadState::Completed, Some(300), Some(300), None);
        job.update_job_from_sub_tasks();
        assert_eq!(job.state, LoadState::Pending);
        assert_eq!(job.total_size, 600);
        assert_eq!(job.loaded_size, 600);
    }
}
*/
