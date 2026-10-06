use clap::Parser;
use curvine_client_core::file::FsClient;
use curvine_core_error::{err_box, CommonResult};
use curvine_fs_api::Path;
use curvine_model::{FileBlockDetails, FileStatus, FileType, WorkerStatus};
use curvine_runtime::common::ByteUnit;
use std::fmt::Write;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct FsckCommand {
    #[clap(value_name = "path")]
    pub path: String,
}

#[derive(Default)]
struct ReplicaSummary {
    block_count: usize,
    expected_replicas: usize,
    recorded_replicas: usize,
    available_replicas: usize,
    unavailable_replicas: usize,
    under_replicated_blocks: usize,
}

impl ReplicaSummary {
    fn has_warnings(&self) -> bool {
        self.under_replicated_blocks > 0 || self.unavailable_replicas > 0
    }
}

impl FsckCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        print!("{}", self.render(client).await?);
        Ok(())
    }

    async fn render(&self, client: Arc<FsClient>) -> CommonResult<String> {
        let path = Path::from_str(&self.path)?;
        let status = client.file_status(&path).await?;
        if status.is_dir {
            return err_box!("fsck directory traversal is not supported yet; pass a file path");
        }

        let details = if should_inspect_blocks(&status) {
            client.get_file_block_details(&path).await?
        } else {
            FileBlockDetails {
                status,
                blocks: Vec::new(),
            }
        };
        Ok(render_file(details, true))
    }
}

fn should_inspect_blocks(status: &FileStatus) -> bool {
    status.file_type == FileType::File && !status.storage_policy.ufs_only()
}

fn summarize_file(details: &mut FileBlockDetails) -> ReplicaSummary {
    details.blocks.sort_by_key(|block| block.offset);
    let mut summary = ReplicaSummary::default();
    for block in &mut details.blocks {
        block.replicas.sort_by_key(|replica| replica.worker_id);
        let expected = details.status.replicas.max(0) as usize;
        let available = block
            .replicas
            .iter()
            .filter(|replica| replica_is_available(replica.state))
            .count();

        summary.block_count += 1;
        summary.expected_replicas += expected;
        summary.recorded_replicas += block.replicas.len();
        summary.available_replicas += available;
        summary.unavailable_replicas += block.replicas.len().saturating_sub(available);
        if available < expected {
            summary.under_replicated_blocks += 1;
        }
    }
    summary
}

fn render_file(mut details: FileBlockDetails, include_status: bool) -> String {
    let summary = summarize_file(&mut details);
    let mut output = String::new();
    writeln!(output, "File: {}", details.status.path).unwrap();
    writeln!(
        output,
        "Size: {} | Blocks: {} | Expected replicas: {}",
        ByteUnit::byte_to_string(details.status.len.max(0) as u64),
        details.blocks.len(),
        details.status.replicas.max(0)
    )
    .unwrap();

    for (index, block) in details.blocks.iter().enumerate() {
        writeln!(
            output,
            "\nBlock {} (blk_{}): {}",
            index + 1,
            block.block_id,
            ByteUnit::byte_to_string(block.len.max(0) as u64)
        )
        .unwrap();
        if block.replicas.is_empty() {
            writeln!(output, "  no replicas").unwrap();
        }
        for replica in &block.replicas {
            let worker = replica
                .address
                .as_ref()
                .map(|address| format!("{}:{}", address.hostname, address.rpc_port))
                .unwrap_or_else(|| format!("worker-{}", replica.worker_id));
            let availability = match replica.state {
                WorkerStatus::Live => "live",
                WorkerStatus::Blacklist => "blacklist",
                WorkerStatus::Decommission => "decommission",
                WorkerStatus::Lost => "lost",
                WorkerStatus::Unknown => "unknown",
            };
            writeln!(output, "  {:<28} {}", worker, availability).unwrap();
        }
    }

    if include_status {
        writeln!(
            output,
            "\nStatus: {}",
            if summary.has_warnings() {
                "WARNING"
            } else {
                "OK"
            }
        )
        .unwrap();
    }
    output
}

fn replica_is_available(status: WorkerStatus) -> bool {
    !matches!(status, WorkerStatus::Lost | WorkerStatus::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{
        BlockReplicaDetail, FileBlockDetail, StoragePolicy, StorageState, WorkerAddress,
    };

    fn address(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            hostname: format!("worker-{worker_id}"),
            rpc_port: 50010,
            ..Default::default()
        }
    }

    fn details(
        replicas: i32,
        replicas_info: Vec<(Option<WorkerAddress>, WorkerStatus)>,
    ) -> FileBlockDetails {
        FileBlockDetails {
            status: FileStatus {
                path: "/data/file".to_string(),
                len: 100,
                replicas,
                ..Default::default()
            },
            blocks: vec![FileBlockDetail {
                block_id: 7,
                len: 100,
                offset: 0,
                replicas: replicas_info
                    .into_iter()
                    .enumerate()
                    .map(|(index, (address, state))| BlockReplicaDetail {
                        worker_id: index as u32 + 1,
                        storage_type: Default::default(),
                        address,
                        state,
                    })
                    .collect(),
            }],
        }
    }

    #[test]
    fn healthy_and_empty_files_have_no_warnings() {
        let mut healthy = details(
            2,
            vec![
                (Some(address(1)), WorkerStatus::Live),
                (Some(address(2)), WorkerStatus::Live),
            ],
        );
        let summary = summarize_file(&mut healthy);
        assert_eq!(summary.expected_replicas, 2);
        assert_eq!(summary.recorded_replicas, 2);
        assert_eq!(summary.available_replicas, 2);
        assert!(!summary.has_warnings());

        let mut empty = details(2, Vec::new());
        empty.status.len = 0;
        empty.blocks.clear();
        let empty_summary = summarize_file(&mut empty);
        assert_eq!(empty_summary.expected_replicas, 0);
        assert!(!empty_summary.has_warnings());
    }

    #[test]
    fn missing_and_unavailable_replicas_are_accounted_separately() {
        let mut details = details(
            3,
            vec![
                (Some(address(1)), WorkerStatus::Live),
                (None, WorkerStatus::Unknown),
            ],
        );
        let summary = summarize_file(&mut details);

        assert_eq!(summary.expected_replicas, 3);
        assert_eq!(summary.recorded_replicas, 2);
        assert_eq!(summary.available_replicas, 1);
        assert_eq!(summary.unavailable_replicas, 1);
        assert_eq!(summary.under_replicated_blocks, 1);
        assert!(summary.has_warnings());
    }

    #[test]
    fn zero_recorded_replicas_are_under_replicated() {
        let mut details = details(1, Vec::new());
        let summary = summarize_file(&mut details);
        assert_eq!(summary.recorded_replicas, 0);
        assert_eq!(summary.under_replicated_blocks, 1);
    }

    #[test]
    fn summarize_file_sorts_blocks_and_replicas() {
        let mut unordered = details(2, vec![(Some(address(1)), WorkerStatus::Live)]);
        unordered.blocks = vec![
            FileBlockDetail {
                block_id: 8,
                len: 100,
                offset: 100,
                replicas: Vec::new(),
            },
            FileBlockDetail {
                block_id: 7,
                len: 100,
                offset: 0,
                replicas: vec![
                    BlockReplicaDetail {
                        worker_id: 3,
                        storage_type: Default::default(),
                        address: None,
                        state: WorkerStatus::Unknown,
                    },
                    BlockReplicaDetail {
                        worker_id: 1,
                        storage_type: Default::default(),
                        address: None,
                        state: WorkerStatus::Unknown,
                    },
                ],
            },
        ];

        summarize_file(&mut unordered);

        assert_eq!(unordered.blocks[0].block_id, 7);
        assert_eq!(unordered.blocks[1].block_id, 8);
        assert_eq!(unordered.blocks[0].replicas[0].worker_id, 1);
        assert_eq!(unordered.blocks[0].replicas[1].worker_id, 3);
    }

    #[test]
    fn file_report_shows_health_without_storage_analysis() {
        let output = render_file(
            details(
                2,
                vec![
                    (Some(address(1)), WorkerStatus::Live),
                    (Some(address(2)), WorkerStatus::Lost),
                ],
            ),
            true,
        );
        assert!(output.contains("worker-1:50010"));
        assert!(output.contains("live"));
        assert!(output.contains("worker-2"));
        assert!(output.contains("lost"));
        assert!(!output.contains("MISMATCH"));
        assert!(!output.contains("Policy:"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn addressed_but_unavailable_replica_is_not_counted_available() {
        let mut details = details(1, vec![(Some(address(1)), WorkerStatus::Lost)]);
        let summary = summarize_file(&mut details);

        assert_eq!(summary.recorded_replicas, 1);
        assert_eq!(summary.available_replicas, 0);
        assert_eq!(summary.unavailable_replicas, 1);
        assert_eq!(summary.under_replicated_blocks, 1);
        assert!(summary.has_warnings());

        let output = render_file(details, true);
        assert!(output.contains("worker-1:50010"));
        assert!(output.contains("lost"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn registered_non_live_worker_statuses_are_counted_available() {
        let mut details = details(
            2,
            vec![
                (Some(address(1)), WorkerStatus::Blacklist),
                (Some(address(2)), WorkerStatus::Decommission),
            ],
        );
        let summary = summarize_file(&mut details);

        assert_eq!(summary.recorded_replicas, 2);
        assert_eq!(summary.available_replicas, 2);
        assert_eq!(summary.unavailable_replicas, 0);
        assert_eq!(summary.under_replicated_blocks, 0);
        assert!(!summary.has_warnings());

        let output = render_file(details, true);
        assert!(output.contains("blacklist"));
        assert!(output.contains("decommission"));
        assert!(output.contains("Status: OK"));
    }

    #[test]
    fn only_regular_non_ufs_files_are_block_inspected() {
        let mut status = FileStatus::default();
        assert!(should_inspect_blocks(&status));

        status.storage_policy = StoragePolicy {
            state: StorageState::Both,
            ..Default::default()
        };
        assert!(should_inspect_blocks(&status));

        for file_type in [
            FileType::Dir,
            FileType::Link,
            FileType::Stream,
            FileType::Agg,
            FileType::Object,
            FileType::Fifo,
            FileType::Char,
            FileType::Block,
            FileType::Socket,
        ] {
            status.file_type = file_type;
            assert!(!should_inspect_blocks(&status), "inspected {file_type:?}");
        }

        status.file_type = FileType::File;
        status.storage_policy = StoragePolicy {
            state: StorageState::Ufs,
            ..Default::default()
        };
        assert!(!should_inspect_blocks(&status));
    }
}
