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

use curvine_cli::cmds::FsckCommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn fsck_style_traversal_pages_nested_directories_without_duplicates() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.client.block_size = 64 * 1024;
            conf.client.block_size_str = "64KB".to_string();
            conf.master.min_block_size = 64 * 1024;
        })
        .build()?;
    let _cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        for path in [
            "/fsck-pages/below/file-1",
            "/fsck-pages/below/file-2",
            "/fsck-pages/exact/file-1",
            "/fsck-pages/exact/file-2",
            "/fsck-pages/exact/file-3",
            "/fsck-pages/over/file-1",
            "/fsck-pages/over/file-2",
            "/fsck-pages/over/file-3",
            "/fsck-pages/over/file-4",
            "/fsck-pages/over/nested/file-5",
        ] {
            fs.write_string(&Path::from_str(path)?, path).await?;
        }

        for (root, expected) in [
            ("/fsck-pages/below", 2),
            ("/fsck-pages/exact", 3),
            ("/fsck-pages/over", 5),
        ] {
            let command = FsckCommand {
                path: root.to_string(),
                detail: true,
                list_page_size: 3,
            };
            let output = command.render(fs.fs_client()).await?;

            assert!(output.contains(&format!("Files: {expected} | Blocks: {expected}")));
            assert!(output.contains("Status: OK"));

            for suffix in expected_paths(root) {
                assert!(
                    output.contains(&format!("File: {suffix}")),
                    "missing {suffix} in:\n{output}"
                );
            }

            let summary = FsckCommand {
                path: root.to_string(),
                detail: false,
                list_page_size: 3,
            }
            .render(fs.fs_client())
            .await?;

            assert!(summary.contains(&format!("Files: {expected} | Blocks: {expected}")));
            assert!(summary.contains(&format!("Expected replicas: {expected}")));
            assert!(summary.contains(&format!("Recorded replicas: {expected}")));
            assert!(summary.contains(&format!("Available replicas: {expected}")));
            assert!(summary.contains("Unavailable replicas: 0"));
            assert!(summary.contains("Under-replicated blocks: 0"));
            assert!(summary.contains("Status: OK"));
            assert!(
                !summary.contains("File: "),
                "unexpected detail output:\n{summary}"
            );
        }
        Ok(())
    })
}

#[test]
fn fsck_reports_single_file_status_and_skipped_directory_entries() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.client.block_size = 64 * 1024;
            conf.client.block_size_str = "64KB".to_string();
            conf.master.min_block_size = 64 * 1024;
        })
        .build()?;
    let _cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let file = Path::from_str("/fsck-skips/file")?;
        let link = Path::from_str("/fsck-skips/link")?;
        fs.write_string(&file, "single file status").await?;
        fs.symlink("/fsck-skips/file", &link, false).await?;

        let file_output = FsckCommand {
            path: file.path().to_string(),
            detail: false,
            list_page_size: 3,
        }
        .render(fs.fs_client())
        .await?;
        assert!(file_output.contains("File: /fsck-skips/file"));
        assert!(file_output.contains("Status: OK"));

        let detail_output = FsckCommand {
            path: "/fsck-skips".to_string(),
            detail: true,
            list_page_size: 3,
        }
        .render(fs.fs_client())
        .await?;
        assert!(detail_output.contains("File: /fsck-skips/link"));
        assert!(detail_output.contains(
            "Block inspection skipped: file type is not a regular file"
        ));

        let summary_output = FsckCommand {
            path: "/fsck-skips".to_string(),
            detail: false,
            list_page_size: 3,
        }
        .render(fs.fs_client())
        .await?;
        assert!(summary_output.contains("Files: 2 | Blocks: 1"));
        assert!(summary_output.contains("Expected replicas: 1"));
        assert!(summary_output.contains("Recorded replicas: 1"));
        assert!(summary_output.contains("Available replicas: 1"));
        assert!(summary_output.contains("Unavailable replicas: 0"));
        assert!(summary_output.contains("Under-replicated blocks: 0"));
        assert!(summary_output.contains("Status: OK"));
        assert!(
            !summary_output.contains("Block inspection skipped:"),
            "unexpected detail output:\n{summary_output}"
        );
        Ok(())
    })
}

#[test]
fn fsck_reports_lost_replica_in_two_worker_cluster() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(2)
        .mutate_conf(|conf| {
            conf.client.replicas = 2;
            conf.client.block_size = 64 * 1024;
            conf.client.block_size_str = "64KB".to_string();
            conf.master.min_block_size = 64 * 1024;
            // Workers are listed as live only after their first heartbeat, so the
            // interval must be short enough for the cluster to become ready. The
            // master requires both worker intervals to be at least the heartbeat
            // interval.
            conf.master.heartbeat_interval = "5s".to_string();
            conf.master.worker_blacklist_interval = "20s".to_string();
            conf.master.worker_lost_interval = "30s".to_string();
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let master = cluster.get_active_master_fs();
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/fsck-multi/file")?;
        fs.write_string(&path, "replicated data").await?;

        let details = fs.fs_client().get_file_block_details(&path).await?;
        assert_eq!(details.blocks.len(), 1);
        assert_eq!(details.blocks[0].replicas.len(), 2);
        let lost_worker_id = details.blocks[0].replicas[0].worker_id;
        assert!(master
            .worker_manager
            .write()
            .remove_expired_worker(lost_worker_id)
            .is_some());

        // The removed worker is still running, so its next heartbeat lists it as
        // live again. That happens at most once per heartbeat interval, so if it
        // lands between the removal and fsck, remove the worker again and retry.
        let mut output = String::new();
        for _ in 0..5 {
            output = FsckCommand {
                path: "/fsck-multi".to_string(),
                detail: true,
                list_page_size: 3,
            }
            .render(fs.fs_client())
            .await?;
            if output.contains("Unavailable replicas: 1") {
                break;
            }
            master
                .worker_manager
                .write()
                .remove_expired_worker(lost_worker_id);
        }

        assert!(output.contains("Files: 1 | Blocks: 1"));
        assert!(output.contains("Expected replicas: 2"));
        assert!(output.contains("Recorded replicas: 2"));
        assert!(output.contains("Available replicas: 1"));
        assert!(output.contains("Unavailable replicas: 1"));
        assert!(output.contains("Under-replicated blocks: 1"));
        assert!(output.contains("lost"));
        assert!(output.contains("live"));
        assert!(output.contains("Status: WARNING"));
        Ok(())
    })
}

fn expected_paths(root: &str) -> &'static [&'static str] {
    match root {
        "/fsck-pages/below" => &["/fsck-pages/below/file-1", "/fsck-pages/below/file-2"],
        "/fsck-pages/exact" => &[
            "/fsck-pages/exact/file-1",
            "/fsck-pages/exact/file-2",
            "/fsck-pages/exact/file-3",
        ],
        "/fsck-pages/over" => &[
            "/fsck-pages/over/file-1",
            "/fsck-pages/over/file-2",
            "/fsck-pages/over/file-3",
            "/fsck-pages/over/file-4",
            "/fsck-pages/over/nested/file-5",
        ],
        _ => &[],
    }
}
