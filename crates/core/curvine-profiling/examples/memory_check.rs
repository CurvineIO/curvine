// Manual verification of the heap profile: jemalloc must be sampling and the
// rendered graph must attribute bytes to our own allocation sites.
// Run with `--features memory-profiling`.
//
// This example installs the profiling allocator itself: a real server gets it
// from `curvine-alloc`, but an example binary has no other consumer to pull
// the `--enable-prof` jemalloc build into the graph.

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
use curvine_config::ProfilingConf;
use curvine_profiling::{memory_available, sample_memory, ProfileTheme};

#[inline(never)]
fn hold_memory(tag: u8, mib: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(mib << 20);
    v.resize(mib << 20, tag);
    v
}

fn main() {
    assert!(memory_available(), "need --features memory-profiling");

    let kept = vec![hold_memory(1, 24), hold_memory(2, 16)];
    std::hint::black_box(&kept);

    let conf = ProfilingConf {
        memory_enabled: true,
        ..Default::default()
    };

    for theme in [ProfileTheme::Light, ProfileTheme::Dark] {
        match sample_memory(&conf, theme) {
            Ok(out) => {
                println!("theme={theme:?} svg={} bytes", out.svg.len());
                let text = String::from_utf8_lossy(&out.svg);
                assert!(text.contains("<svg"), "not an svg");
                let ours = text.matches("hold_memory").count();
                println!("  hold_memory frames = {ours}");
                assert!(ours > 0, "heap graph does not attribute our allocation");
            }
            Err(e) => panic!("theme={theme:?} failed: {e}"),
        }
    }

    // Disabled config must refuse rather than return an empty graph.
    let off = ProfilingConf::default();
    assert!(sample_memory(&off, ProfileTheme::Light).is_err());
    println!("OK");
}
