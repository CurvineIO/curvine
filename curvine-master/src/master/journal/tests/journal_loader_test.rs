use super::*;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use raft::eraftpb::Entry;

#[test]
fn metadata_batch_is_applied_after_versioned_journal_upgrade() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("metadata-batch-source-{}", Utils::rand_str(6)));
    let source_fs = JournalSystem::fs_only_for_test(&source_conf)?;
    source_fs.mkdir("/legacy", false)?;
    let metadata = source_fs
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .next()
        .map(|entry| match entry {
            JournalEntry::Mkdir(entry) => MetadataCommand::Mkdir(entry),
            _ => unreachable!("mkdir must emit a mkdir journal entry"),
        })
        .expect("mkdir must emit a journal entry");

    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("metadata-batch-target-{}", Utils::rand_str(6)));
    let target = JournalSystem::from_conf(&target_conf)?;
    let target_fs = target.fs();

    let mut batch = JournalCommandBatch::new(1);
    batch.push_metadata(metadata);
    let entry = Entry {
        term: 1,
        index: 1,
        data: JournalEnvelope::encode(batch)?,
        ..Default::default()
    };

    AsyncRuntime::single().block_on(async {
        target
            .journal_loader()
            .apply(true, ApplyMsg::new_entry(entry))
            .await
    })?;
    let status = target_fs.file_status("/legacy");
    assert!(status.is_ok(), "metadata mkdir replay failed: {status:?}");

    Ok(())
}
