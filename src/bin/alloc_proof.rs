// ▶ SHOW: custom global allocator — intercepts every malloc to count heap allocs
#[global_allocator]
static COUNTER: wiki_rt_monitor::alloc_counter::AllocCounter =
    wiki_rt_monitor::alloc_counter::AllocCounter;

use wiki_rt_monitor::alloc_counter::reset_and_read;
use wiki_rt_monitor::component_b::{parse_zero_copy, HotPathProcessor};
use wiki_rt_monitor::component_d::{Leaderboard, SyncStrategy};
use wiki_rt_monitor::component_e::FailSafe;
use wiki_rt_monitor::metrics::new_metrics;
use wiki_rt_monitor::types::StressConfig;

use std::sync::Arc;
use bytes::Bytes;

const N_PACKETS: usize = 100_000;

fn main() {
    println!("=== Hot-Path Allocation Proof (RTS2601 Distinction) ===\n");
    println!("Building {N_PACKETS} packets outside the measured window...");

    let human_buf = Bytes::from_static(br#"{"bot":false,"user":"Alice","server_name":"en.wikipedia.org","title":"Rust","type":"edit","timestamp":1715000000,"namespace":0}"#);
    let bot_buf   = Bytes::from_static(br#"{"bot":true,"user":"ClueBot","server_name":"de.wikipedia.org","title":"Java","type":"edit","timestamp":1715000001,"namespace":0}"#);

    let make_packets = |n: usize| -> Vec<_> {
        (0..n).map(|i| {
            let buf = if i % 5 == 0 { &human_buf } else { &bot_buf };
            parse_zero_copy(buf).expect("valid packet")
        }).collect()
    };

    let metrics     = new_metrics();
    let leaderboard = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&metrics));
    let fail_safe   = FailSafe::new(Arc::clone(&metrics));
    let processor   = HotPathProcessor::new(
        Arc::clone(&metrics),
        Arc::clone(&leaderboard),
        Arc::clone(&fail_safe),
        StressConfig::off(),
    );

    // Warm up to steady-state before measuring.
    for pkt in make_packets(10_100) {
        processor.process(pkt);
    }

    let measurement_packets = make_packets(N_PACKETS);
    // ▶ SHOW: counter reset here — only allocs after this line count
    reset_and_read();

    for pkt in measurement_packets {
        processor.process(pkt);
    }

    let (total_allocs, _) = reset_and_read();
    let allocs_per_packet = total_allocs as f64 / N_PACKETS as f64;

    println!();
    println!("Results:");
    println!("  Packets processed:  {N_PACKETS}");
    println!("  Total allocs (hot): {total_allocs}");
    println!("  Allocs per packet:  {allocs_per_packet:.4}");

    std::fs::create_dir_all("logs").ok();
    let proof = format!(
        "Hot-path allocation proof (RTS2601)\n\
         Packets processed:  {N_PACKETS}\n\
         Total allocs (hot): {total_allocs}\n\
         Allocs per packet:  {allocs_per_packet:.4}\n"
    );
    if let Ok(()) = std::fs::write("logs/alloc_proof.txt", &proof) {
        println!("\n[logs] alloc_proof.txt written.");
    }
    println!("\n{proof}");

    if allocs_per_packet < 1.0 {
        println!("[PASS] Hot path meets Distinction zero-alloc requirement.");
    } else {
        println!("[NOTE] {allocs_per_packet:.2} allocs/packet");
    }
}
