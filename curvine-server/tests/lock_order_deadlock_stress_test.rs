use curvine_common::conf::{ClusterConf, JournalConf, MasterConf};
use curvine_common::state::{
    BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, StorageType, WorkerInfo,
};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use orpc::common::Utils;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc,
};
use std::thread;
use std::time::{Duration, Instant};

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem) {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("deadlock-stress/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("deadlock-stress/journal-{}", name)),
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
fn stress_add_block_vs_block_report_no_hang() {
    let (fs, _js) = new_fs("api");
    fs.create("/deadlock/file.log", true).unwrap();

    let fs = Arc::new(fs);
    let loops = 4000usize;
    let timeout = Duration::from_secs(20);
    let start = Instant::now();

    let (tx, rx) = mpsc::channel::<&'static str>();
    let add_progress = Arc::new(AtomicUsize::new(0));
    let report_progress = Arc::new(AtomicUsize::new(0));

    let fs_a = fs.clone();
    let tx_a = tx.clone();
    let add_progress_a = add_progress.clone();
    let t1 = thread::spawn(move || {
        let client = ClientAddress {
            client_name: "deadlock-repro".into(),
            hostname: "localhost".into(),
            ip_addr: "127.0.0.1".into(),
            port: 0,
        };

        let mut ok = 0usize;
        let mut err = 0usize;
        for i in 0..loops {
            let res = fs_a.add_block(
                "/deadlock/file.log",
                client.clone(),
                vec![],
                vec![],
                0,
                None,
            );
            if res.is_ok() {
                ok += 1;
            } else {
                err += 1;
            }
            if i % 10 == 0 {
                add_progress_a.store(i, Ordering::Relaxed);
            }
        }
        add_progress_a.store(loops, Ordering::Relaxed);
        let _ = tx_a.send("add_block_done");
        (ok, err)
    });

    let fs_b = fs.clone();
    let tx_b = tx.clone();
    let report_progress_b = report_progress.clone();
    let t2 = thread::spawn(move || {
        let mut ok = 0usize;
        let mut err = 0usize;
        for i in 0..loops {
            let report = BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 100,
                full_report: false,
                total_len: 0,
                blocks: vec![BlockReportInfo::new(
                    9_000_000 + i as i64,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    0,
                )],
            };

            let res = fs_b.block_report(report);
            if res.is_ok() {
                ok += 1;
            } else {
                err += 1;
            }
            if i % 10 == 0 {
                report_progress_b.store(i, Ordering::Relaxed);
            }
        }
        report_progress_b.store(loops, Ordering::Relaxed);
        let _ = tx_b.send("block_report_done");
        (ok, err)
    });

    let first = rx.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!(
            "timeout waiting first worker, add_progress={}, report_progress={}",
            add_progress.load(Ordering::Relaxed),
            report_progress.load(Ordering::Relaxed)
        )
    });
    let second = rx.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!(
            "timeout waiting second worker, add_progress={}, report_progress={}",
            add_progress.load(Ordering::Relaxed),
            report_progress.load(Ordering::Relaxed)
        )
    });

    let (a_ok, a_err) = t1.join().unwrap();
    let (b_ok, b_err) = t2.join().unwrap();

    eprintln!(
        "finished in {:?}, done=({}, {}), add_block ok/err={}/{}, block_report ok/err={}/{}",
        start.elapsed(),
        first,
        second,
        a_ok,
        a_err,
        b_ok,
        b_err
    );
}

#[test]
fn sanity_single_thread_paths_progress() {
    let (fs, _js) = new_fs("sanity");
    fs.create("/deadlock/file.log", true).unwrap();

    let client = ClientAddress {
        client_name: "sanity".into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        port: 0,
    };

    for i in 0..500 {
        let _ = fs.add_block(
            "/deadlock/file.log",
            client.clone(),
            vec![],
            vec![],
            0,
            None,
        );
        let report = BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                8_000_000 + i as i64,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                0,
            )],
        };
        let _ = fs.block_report(report);
    }
}
