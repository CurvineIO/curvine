//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#[cfg(target_os = "linux")]
#[test]
fn persist_restore() {
    use std::sync::Arc;

    use curvine_common::fs::{Path, StateReader, StateWriter};
    use curvine_common::state::{CreateFileOptsBuilder, FileStatus, OpenFlags};
    use curvine_fuse::fs::state::NodeState;
    use curvine_fuse::FUSE_ROOT_ID;
    use curvine_tests::Testing;
    use orpc::common::Utils;
    use orpc::runtime::AsyncRuntime;
    use orpc::runtime::RpcRuntime;

    let rt = Arc::new(AsyncRuntime::single());
    let testing: Testing = Testing::default();
    let rt1 = rt.clone();

    rt1.block_on(async move {
        let test_path = Utils::test_file();

        // Create original NodeState and add data
        let fs1 = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
        let state1 = NodeState::new(fs1.clone());

        // Add some nodes
        let status_a = FileStatus::with_name(2, "a".to_string(), true);
        let status_b = FileStatus::with_name(3, "b".to_string(), true);
        let status_c = FileStatus::with_name(4, "c".to_string(), true);
        let a = state1
            .do_lookup(FUSE_ROOT_ID, Some("a"), &status_a)
            .unwrap();
        let b = state1.do_lookup(a.ino, Some("b"), &status_b).unwrap();
        let c = state1.do_lookup(b.ino, Some("c"), &status_c).unwrap();

        // Create dir_handles
        let dir_status_list = vec![
            FileStatus::with_name(10, "file1".to_string(), false),
            FileStatus::with_name(11, "file2".to_string(), false),
        ];
        let dir_handle1 = state1
            .new_dir_handle(a.ino, dir_status_list.clone())
            .await
            .unwrap();
        let dir_handle2 = state1
            .new_dir_handle(b.ino, dir_status_list.clone())
            .await
            .unwrap();

        // Create file handles
        let path = Path::from_str("/a/1.log").unwrap();
        let handle1 = state1
            .new_handle(
                21,
                &path,
                OpenFlags::new_create().value(),
                CreateFileOptsBuilder::with_conf(state1.client_conf())
                    .create_parent(true)
                    .build(),
            )
            .await
            .unwrap();

        let path = Path::from_str("/a/2.log").unwrap();
        let handle2 = state1
            .new_handle(
                22,
                &path,
                OpenFlags::new_create().set_read_write().value(),
                CreateFileOptsBuilder::with_conf(state1.client_conf())
                    .create_parent(true)
                    .build(),
            )
            .await
            .unwrap();

        // Add locks to handle1 for testing
        handle1.add_lock(curvine_common::state::LockFlags::Flock, 100);
        handle1.add_lock(curvine_common::state::LockFlags::Plock, 200);

        // Record original state for comparison
        let original_node_count = state1.node_read().nodes_len();
        let original_id_creator = state1.node_read().current_id();
        let original_fh_creator = state1.current_fh();
        let original_handle1_status = handle1.status().clone();
        let _original_dir_handle1_list = dir_handle1.get_all().to_vec();

        // Persist state
        let mut writer = StateWriter::new(&test_path).unwrap();
        state1.persist(&mut writer).await.unwrap();
        drop(writer);

        // Create new NodeState and restore
        let fs2 = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
        let state2 = NodeState::new(fs2);

        let mut reader = StateReader::new(&test_path).unwrap();
        state2.restore(&mut reader).await.unwrap();

        // Verify node paths
        let path_a = state2.get_path(a.ino).unwrap();
        assert_eq!(path_a.path(), "/a");

        let path_b = state2.get_path(b.ino).unwrap();
        assert_eq!(path_b.path(), "/a/b");

        let path_c = state2.get_path(c.ino).unwrap();
        assert_eq!(path_c.path(), "/a/b/c");

        // Verify node lookup
        let found_a = state2.find_node(FUSE_ROOT_ID, Some("a")).unwrap();
        assert_eq!(found_a.id, a.ino);

        let found_b = state2.find_node(a.ino, Some("b")).unwrap();
        assert_eq!(found_b.id, b.ino);

        let found_c = state2.find_node(b.ino, Some("c")).unwrap();
        assert_eq!(found_c.id, c.ino);

        // Verify handle restoration
        assert_eq!(state2.all_handles().len(), state1.all_handles().len());
        assert!(state2.find_handle(handle1.ino, handle1.fh).is_ok());
        assert!(state2.find_handle(handle2.ino, handle2.fh).is_ok());

        // Verify dir_handle restoration
        assert_eq!(
            state2.all_dir_handles().len(),
            state1.all_dir_handles().len()
        );
        let restored_dir_handle1 = state2
            .find_dir_handle(dir_handle1.ino, dir_handle1.fh)
            .unwrap();
        assert_eq!(restored_dir_handle1.ino, dir_handle1.ino);
        assert_eq!(restored_dir_handle1.fh, dir_handle1.fh);
        assert_eq!(restored_dir_handle1.len(), dir_handle1.len());

        let restored_dir_handle2 = state2
            .find_dir_handle(dir_handle2.ino, dir_handle2.fh)
            .unwrap();
        assert_eq!(restored_dir_handle2.ino, dir_handle2.ino);
        assert_eq!(restored_dir_handle2.fh, dir_handle2.fh);

        // Verify file handle details
        let restored_handle1 = state2.find_handle(handle1.ino, handle1.fh).unwrap();
        assert_eq!(restored_handle1.ino, handle1.ino);
        assert_eq!(restored_handle1.fh, handle1.fh);
        assert_eq!(restored_handle1.status().path, original_handle1_status.path);
        assert_eq!(restored_handle1.status().id, original_handle1_status.id);

        // Verify locks are restored (check that locks exist by trying to remove them)
        let flock_owner = restored_handle1.remove_lock(curvine_common::state::LockFlags::Flock);
        assert_eq!(flock_owner, Some(100));
        let plock_owner = restored_handle1.remove_lock(curvine_common::state::LockFlags::Plock);
        assert_eq!(plock_owner, Some(200));

        // Verify counters
        let restored_node_count = state2.node_read().nodes_len();
        let restored_id_creator = state2.node_read().current_id();
        let restored_fh_creator = state2.current_fh();
        assert_eq!(restored_node_count, original_node_count);
        assert_eq!(restored_id_creator, original_id_creator);
        assert_eq!(restored_fh_creator, original_fh_creator);

        // Clean up test file
        let _ = std::fs::remove_file(&test_path);
    });
}
