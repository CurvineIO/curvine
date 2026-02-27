use curvine_common::conf::{ClusterConf, JournalConf, MasterConf};
use curvine_common::state::WorkerInfo;
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use orpc::common::Utils;
use std::sync::{Arc, Barrier};
use std::thread;

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem) {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("deadlock-repro/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("deadlock-repro/journal-{}", name)),
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
fn test_lock_order_inversion_window_exists() {
    let (fs, _js) = new_fs("lock-order");
    let fs = Arc::new(fs);

    let barrier = Arc::new(Barrier::new(3));

    let fs_a = fs.clone();
    let b1 = barrier.clone();
    let t1 = thread::spawn(move || {
        let _fs_guard = fs_a.fs_dir.write();
        b1.wait();
        fs_a.worker_manager.try_read().is_err()
    });

    let fs_b = fs.clone();
    let b2 = barrier.clone();
    let t2 = thread::spawn(move || {
        let _wm_guard = fs_b.worker_manager.write();
        b2.wait();
        fs_b.fs_dir.try_read().is_err()
    });

    barrier.wait();

    let a_blocked = t1.join().unwrap();
    let b_blocked = t2.join().unwrap();

    assert!(
        a_blocked && b_blocked,
        "expected both cross-lock acquisitions to be blocked under opposite lock ordering"
    );
}
