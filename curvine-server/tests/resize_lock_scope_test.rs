use curvine_common::conf::{ClusterConf, JournalConf, MasterConf};
use curvine_common::state::{ClientAddress, FileAllocOpts, WorkerInfo};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use orpc::common::Utils;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem) {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("resize-lock-scope/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("resize-lock-scope/journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };

    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());
    (fs, js)
}

#[test]
fn resize_does_not_hold_fs_lock_while_waiting_worker_manager() {
    let (fs, _js) = new_fs("scope");
    fs.create("/resize/file.log", true).unwrap();

    let client = ClientAddress {
        client_name: "resize-lock-scope".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };
    fs.add_block("/resize/file.log", client, vec![], vec![], 0, None)
        .unwrap();

    let fs = Arc::new(fs);

    let fs_guard = fs.fs_dir.write();
    let wm_guard = fs.worker_manager.write();

    let fs_resize = fs.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let t = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let res = fs_resize.resize("/resize/file.log", FileAllocOpts::with_truncate(0));
        done_tx.send(res.is_ok()).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        done_rx.try_recv().is_err(),
        "resize should not complete before fs_dir/worker_manager locks are released"
    );

    drop(fs_guard);
    thread::sleep(Duration::from_millis(80));

    assert!(
        done_rx.try_recv().is_err(),
        "resize should still be blocked while worker_manager write lock is held"
    );

    assert!(
        fs.fs_dir.try_read().is_ok(),
        "resize must not keep fs_dir locked while waiting on worker_manager"
    );

    drop(wm_guard);

    assert!(
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        "resize should complete after worker_manager lock is released"
    );
    t.join().unwrap();
}
