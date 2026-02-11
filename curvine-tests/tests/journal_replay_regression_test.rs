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

use curvine_common::conf::ClusterConf;
use curvine_common::state::{BlockLocation, ClientAddress, CommitBlock, WorkerInfo};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::Master;
use orpc::common::{LocalTime, Logger, Utils};
use orpc::{err_box, CommonResult};

#[test]
fn test_replay_skips_stale_add_block_after_delete() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };

    let case_name = format!(
        "curvine-tests-issue-649-{}-{}",
        LocalTime::mills(),
        Utils::rand_str(6)
    );
    conf.change_test_meta_dir(&case_name);

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());

    let path = "/issue649-stale-add-block.log";
    fs.create(path, true)?;
    fs.add_block(path, ClientAddress::default(), vec![], vec![], 0, None)?;
    fs.delete(path, true)?;

    let mut create_entry: Option<JournalEntry> = None;
    let mut delete_entry: Option<JournalEntry> = None;
    let mut add_block_entry: Option<JournalEntry> = None;

    for entry in js.fs().fs_dir.read().take_entries() {
        match entry {
            JournalEntry::CreateFile(_) if create_entry.is_none() => {
                create_entry = Some(entry);
            }
            JournalEntry::Delete(_) if delete_entry.is_none() => {
                delete_entry = Some(entry);
            }
            JournalEntry::AddBlock(_) if add_block_entry.is_none() => {
                add_block_entry = Some(entry);
            }
            _ => {}
        }
    }

    let create_entry = match create_entry {
        Some(entry) => entry,
        None => return err_box!("missing CreateFile journal entry"),
    };
    let delete_entry = match delete_entry {
        Some(entry) => entry,
        None => return err_box!("missing Delete journal entry"),
    };
    let add_block_entry = match add_block_entry {
        Some(entry) => entry,
        None => return err_box!("missing AddBlock journal entry"),
    };

    let mut replay_conf = conf.clone();
    replay_conf.change_test_meta_dir(format!("{}-replay", case_name));

    let replay_js = JournalSystem::from_conf(&replay_conf)?;
    let replay_fs = MasterFilesystem::with_js(&replay_conf, &replay_js);
    let loader = JournalLoader::new(
        replay_fs.fs_dir(),
        replay_js.mount_manager(),
        &replay_conf.journal,
    );

    loader.apply_entry(create_entry)?;
    loader.apply_entry(delete_entry)?;

    // Simulate stale AddBlock log replaying after snapshot state where file was already deleted.
    let replay_res = loader.apply_entry(add_block_entry);
    assert!(
        replay_res.is_ok(),
        "stale AddBlock should be skipped during replay, got: {:?}",
        replay_res
    );
    assert!(!replay_fs.exists(path)?);

    Ok(())
}

#[test]
fn test_replay_skips_stale_complete_file_after_delete() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };

    let case_name = format!(
        "curvine-tests-issue-649-complete-{}-{}",
        LocalTime::mills(),
        Utils::rand_str(6)
    );
    conf.change_test_meta_dir(&case_name);

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());

    let path = "/issue649-stale-complete-file.log";
    let address = ClientAddress::default();
    fs.create(path, true)?;
    let add_block = fs.add_block(path, address.clone(), vec![], vec![], 0, None)?;
    let commit = CommitBlock {
        block_id: add_block.block.id,
        block_len: add_block.block.len,
        locations: vec![BlockLocation::with_id(add_block.locs[0].worker_id)],
    };
    fs.complete_file(
        path,
        add_block.block.len,
        vec![commit],
        &address.client_name,
        false,
    )?;
    fs.delete(path, true)?;

    let mut create_entry: Option<JournalEntry> = None;
    let mut delete_entry: Option<JournalEntry> = None;
    let mut complete_entry: Option<JournalEntry> = None;

    for entry in js.fs().fs_dir.read().take_entries() {
        match entry {
            JournalEntry::CreateFile(_) if create_entry.is_none() => {
                create_entry = Some(entry);
            }
            JournalEntry::Delete(_) if delete_entry.is_none() => {
                delete_entry = Some(entry);
            }
            JournalEntry::CompleteFile(_) if complete_entry.is_none() => {
                complete_entry = Some(entry);
            }
            _ => {}
        }
    }

    let create_entry = match create_entry {
        Some(entry) => entry,
        None => return err_box!("missing CreateFile journal entry"),
    };
    let delete_entry = match delete_entry {
        Some(entry) => entry,
        None => return err_box!("missing Delete journal entry"),
    };
    let complete_entry = match complete_entry {
        Some(entry) => entry,
        None => return err_box!("missing CompleteFile journal entry"),
    };

    let mut replay_conf = conf.clone();
    replay_conf.change_test_meta_dir(format!("{}-replay", case_name));

    let replay_js = JournalSystem::from_conf(&replay_conf)?;
    let replay_fs = MasterFilesystem::with_js(&replay_conf, &replay_js);
    let loader = JournalLoader::new(
        replay_fs.fs_dir(),
        replay_js.mount_manager(),
        &replay_conf.journal,
    );

    loader.apply_entry(create_entry)?;
    loader.apply_entry(delete_entry)?;

    // Simulate stale CompleteFile replaying after snapshot state where file was already deleted.
    let replay_res = loader.apply_entry(complete_entry);
    assert!(
        replay_res.is_ok(),
        "stale CompleteFile should be skipped during replay, got: {:?}",
        replay_res
    );
    assert!(!replay_fs.exists(path)?);

    Ok(())
}

#[test]
fn test_replay_skips_stale_delete_when_path_missing() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };

    let case_name = format!(
        "curvine-tests-issue-649-delete-{}-{}",
        LocalTime::mills(),
        Utils::rand_str(6)
    );
    conf.change_test_meta_dir(&case_name);

    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);

    let path = "/issue649-stale-delete.log";
    fs.create(path, true)?;
    fs.delete(path, true)?;

    let mut delete_entry: Option<JournalEntry> = None;
    for entry in js.fs().fs_dir.read().take_entries() {
        if let JournalEntry::Delete(_) = entry {
            delete_entry = Some(entry);
            break;
        }
    }

    let delete_entry = match delete_entry {
        Some(entry) => entry,
        None => return err_box!("missing Delete journal entry"),
    };

    let mut replay_conf = conf.clone();
    replay_conf.change_test_meta_dir(format!("{}-replay", case_name));

    let replay_js = JournalSystem::from_conf(&replay_conf)?;
    let replay_fs = MasterFilesystem::with_js(&replay_conf, &replay_js);
    let loader = JournalLoader::new(
        replay_fs.fs_dir(),
        replay_js.mount_manager(),
        &replay_conf.journal,
    );

    // Simulate stale Delete replaying after snapshot state where path is already absent.
    let replay_res = loader.apply_entry(delete_entry);
    assert!(
        replay_res.is_ok(),
        "stale Delete should be skipped during replay, got: {:?}",
        replay_res
    );
    assert!(!replay_fs.exists(path)?);

    Ok(())
}
