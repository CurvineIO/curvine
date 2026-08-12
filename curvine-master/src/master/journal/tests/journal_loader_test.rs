use super::*;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use raft::eraftpb::Entry;

#[test]
fn metadata_batch_is_rejected_before_legacy_commands_are_applied() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("metadata-batch-source-{}", Utils::rand_str(6)));
    let source_fs = JournalSystem::fs_only_for_test(&source_conf)?;
    source_fs.mkdir("/legacy", false)?;
    let legacy = source_fs
        .fs_dir
        .read()
        .take_entries()
        .into_iter()
        .next()
        .expect("mkdir must emit a journal entry");
    let metadata = match legacy.clone() {
        JournalEntry::Mkdir(entry) => MetadataCommand::Mkdir(entry),
        _ => unreachable!("mkdir must emit a mkdir journal entry"),
    };

    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("metadata-batch-target-{}", Utils::rand_str(6)));
    let target = JournalSystem::from_conf(&target_conf)?;
    let target_fs = target.fs();

    let mut batch = JournalCommandBatch::new(1);
    batch.push_legacy(legacy);
    batch.push_metadata(metadata);
    let entry = Entry {
        term: 1,
        index: 1,
        data: JournalEnvelope::encode(batch)?,
        ..Default::default()
    };

    let err = AsyncRuntime::single()
        .block_on(async {
            target
                .journal_loader()
                .apply(true, ApplyMsg::new_entry(entry))
                .await
        })
        .expect_err("metadata commands are not supported by this journal version");
    assert!(err
        .to_string()
        .contains("unsupported committed metadata command batch"));
    assert!(target_fs.file_status("/legacy").is_err());

    Ok(())
}
