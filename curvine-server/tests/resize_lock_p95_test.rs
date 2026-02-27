use curvine_common::conf::{ClusterConf, JournalConf, MasterConf};
use curvine_common::state::{FileAllocOpts, WorkerInfo};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::Master;
use orpc::common::Utils;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn new_fs(name: &str) -> (MasterFilesystem, JournalSystem) {
    Master::init_test_metrics();
    let conf = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("resize-p95/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("resize-p95/journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };
    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    fs.add_test_worker(WorkerInfo::default());
    (fs, js)
}

fn pct(v: &mut [u128], q: f64) -> u128 {
    v.sort_unstable();
    let idx = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[idx]
}

#[test]
fn measure_resize_blocks_fs_read_latency() {
    let (fs, _js) = new_fs("quant");
    fs.create("/resize/file.log", true).unwrap();
    let fs = Arc::new(fs);
    let rounds = 80;
    let hold_ms = 12;
    let mut waits_us = Vec::with_capacity(rounds);

    for _ in 0..rounds {
        let fs_holder = fs.clone();
        let holder = thread::spawn(move || {
            let guard = fs_holder.worker_manager.write();
            thread::sleep(Duration::from_millis(hold_ms));
            drop(guard);
        });

        thread::sleep(Duration::from_millis(1));

        let b = Arc::new(Barrier::new(2));
        let fs_resize = fs.clone();
        let b2 = b.clone();
        let resize_t = thread::spawn(move || {
            b2.wait();
            let _ = fs_resize.resize("/resize/file.log", FileAllocOpts::with_truncate(0));
        });

        b.wait();
        thread::sleep(Duration::from_millis(1));

        let st = Instant::now();
        let g = fs.fs_dir.read();
        let elapsed = st.elapsed().as_micros();
        drop(g);
        waits_us.push(elapsed);

        let _ = holder.join();
        let _ = resize_t.join();
    }

    let mut cp = waits_us.clone();
    let p50 = pct(&mut cp, 0.50);
    let p95 = pct(&mut cp, 0.95);
    let p99 = pct(&mut cp, 0.99);
    println!(
        "RESIZE_FS_READ_WAIT_US p50={} p95={} p99={} samples={}",
        p50,
        p95,
        p99,
        waits_us.len()
    );
}
