/// Counting allocator – proof of zero (or minimal) heap allocations on the hot path.
///
/// Wraps the system allocator and counts every alloc/dealloc call via atomics.
/// Enable with feature "alloc-count" or use directly in alloc_proof binary.
///
/// Verification method (Distinction requirement):
///   1. Reset the counter before the hot-path window (T2).
///   2. Run N packets through HotPathProcessor::process (excluding ChangePacket
///      construction, which is outside the hot-path window).
///   3. Check the counter: 0 or O(1) allocs per packet proves zero-copy on path.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Global allocation counter – incremented by AllocCounter on every alloc.
pub static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
/// Global deallocation counter.
pub static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Custom global allocator that counts heap allocations.
pub struct AllocCounter;

unsafe impl GlobalAlloc for AllocCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }
}

/// Reset both counters and return (allocs_since_reset, deallocs_since_reset).
pub fn reset_and_read() -> (u64, u64) {
    let a = ALLOC_COUNT.swap(0, Ordering::Relaxed);
    let d = DEALLOC_COUNT.swap(0, Ordering::Relaxed);
    (a, d)
}

/// Read current counts without resetting.
pub fn read_counts() -> (u64, u64) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        DEALLOC_COUNT.load(Ordering::Relaxed),
    )
}
