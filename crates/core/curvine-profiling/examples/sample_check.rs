// Manual verification: real SIGPROF sampling must produce a usable SVG, and
// the sampler must be reusable. Run with `--features profiling`.
// Not part of CI: signal-driven sampling is not reliable in containers.
use curvine_config::ProfilingConf;
use curvine_profiling::{available, sample, ProfileTheme, ProfilingError};

fn burn(d: std::time::Duration) {
    let t = std::time::Instant::now();
    let mut a = 0u64;
    while t.elapsed() < d {
        a = a.wrapping_mul(6364136223846793005).wrapping_add(1);
    }
    std::hint::black_box(a);
}

fn main() {
    assert!(available(), "run with --features profiling");
    let conf = ProfilingConf {
        cpu_enabled: true,
        max_duration_secs: 10,
        ..Default::default()
    };

    // First session under load.
    let h = std::thread::spawn(|| burn(std::time::Duration::from_secs(4)));
    let first = sample(&conf, Some(2), Some(500), ProfileTheme::Light).expect("sampling failed");
    let _ = h.join();
    let text = String::from_utf8_lossy(&first.svg);
    assert!(text.starts_with("<?xml"), "not an SVG document");
    assert!(text.contains("<svg"), "missing svg root");
    assert!(first.svg.len() > 1000, "suspiciously small flame graph");
    assert_eq!(first.seconds, 2);
    assert_eq!(first.frequency, 500);
    println!(
        "first: seconds={} freq={} bytes={}",
        first.seconds,
        first.frequency,
        first.svg.len()
    );

    // Second session: proves the SIGPROF handler was restored and the busy
    // slot released. This is the open question from the design doc.
    let h = std::thread::spawn(|| burn(std::time::Duration::from_secs(3)));
    let second =
        sample(&conf, Some(1), Some(200), ProfileTheme::Dark).expect("second sampling failed");
    let _ = h.join();
    assert!(second.svg.len() > 1000);
    println!("second: bytes={}", second.svg.len());

    // Idle process: an empty window must be reported, not returned as a
    // zero-frame image that looks like a broken sampler.
    match sample(&conf, Some(1), Some(200), ProfileTheme::Dark) {
        Err(ProfilingError::NoSamples { seconds }) => {
            println!("idle: reported NoSamples({seconds})")
        }
        other => panic!("expected NoSamples, got {:?}", other.map(|o| o.svg.len())),
    }

    println!("OK");
}
