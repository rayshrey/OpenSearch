/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Global memory pool instances for write and merge operations.

use std::sync::{Arc, OnceLock};
use native_bridge_common::memory_pool::MemoryPool;

static WRITE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();
static MERGE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();

/// Returns the node-level write memory pool. Lazily initialized with limit 0 (unlimited).
pub fn write_pool() -> &'static Arc<MemoryPool> {
    WRITE_POOL.get_or_init(|| Arc::new(MemoryPool::new("WRITE", 0)))
}

/// Returns the node-level merge memory pool. Lazily initialized with limit 0 (unlimited).
pub fn merge_pool() -> &'static Arc<MemoryPool> {
    MERGE_POOL.get_or_init(|| Arc::new(MemoryPool::new("MERGE", 0)))
}

/// Initialize both pools with limits. Called from FFI at node startup.
pub fn init_pools(write_limit: usize, merge_limit: usize) {
    write_pool().set_limit(write_limit);
    merge_pool().set_limit(merge_limit);
}

pub fn set_write_limit(v: usize) {
    write_pool().set_limit(v);
}

pub fn set_merge_limit(v: usize) {
    merge_pool().set_limit(v);
}

/// Returns [write_limit, write_used, write_peak, merge_limit, merge_used, merge_peak].
pub fn get_stats() -> [usize; 6] {
    let w = (write_pool().limit(), write_pool().used(), write_pool().peak());
    let m = (merge_pool().limit(), merge_pool().used(), merge_pool().peak());
    [w.0, w.1, w.2, m.0, m.1, m.2]
}
