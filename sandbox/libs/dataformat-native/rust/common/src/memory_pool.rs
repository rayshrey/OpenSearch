/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Memory pool for tracking native memory usage across write and merge operations.
//!
//! Provides a simple atomic counter with an optional limit. Operations that allocate
//! significant memory (RecordBatch buffering, sort read-back, merge cursors) call
//! `try_grow` before allocating and `shrink` after freeing. The pool rejects
//! allocations that would exceed the configured limit.
//!
//! `MemoryReservation` is an RAII handle that automatically returns memory to the
//! pool on drop, preventing leaks even on error paths.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::fmt;
use crate::{log_info, log_error};

/// Error returned when a pool cannot satisfy an allocation request.
#[derive(Debug, Clone)]
pub struct PoolExhausted {
    pub pool_name: &'static str,
    pub requested: usize,
    pub used: usize,
    pub limit: usize,
}

impl fmt::Display for PoolExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] memory limit exceeded: requested {} bytes, used {}, limit {}",
            self.pool_name, self.requested, self.used, self.limit
        )
    }
}

impl std::error::Error for PoolExhausted {}

/// A node-level memory pool backed by an atomic counter.
#[derive(Debug)]
pub struct MemoryPool {
    name: &'static str,
    used: AtomicUsize,
    limit: AtomicUsize,
    peak: AtomicUsize,
}

impl MemoryPool {
    /// Create a new pool. `limit = 0` means unlimited.
    pub fn new(name: &'static str, limit: usize) -> Self {
        Self {
            name,
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(limit),
            peak: AtomicUsize::new(0),
        }
    }

    /// Attempt to reserve `bytes`. Returns error if it would exceed the limit.
    pub fn try_grow(&self, bytes: usize, consumer: &str) -> Result<(), PoolExhausted> {
        if bytes == 0 {
            return Ok(());
        }
        let limit = self.limit.load(Ordering::Relaxed);
        let result = self.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            let new_used = used.checked_add(bytes)?;
            if limit > 0 && new_used > limit {
                None
            } else {
                Some(new_used)
            }
        });

        match result {
            Ok(old) => {
                let new_used = old + bytes;
                self.peak.fetch_max(new_used, Ordering::Relaxed);
                log_info!(
                    "[{}] +{} bytes for '{}' (used: {}, limit: {})",
                    self.name, bytes, consumer, new_used, limit
                );
                Ok(())
            }
            Err(_) => {
                let used = self.used.load(Ordering::Relaxed);
                log_info!(
                    "[{}] REJECTED +{} bytes for '{}' (used: {}, limit: {})",
                    self.name, bytes, consumer, used, limit
                );
                Err(PoolExhausted {
                    pool_name: self.name,
                    requested: bytes,
                    used,
                    limit,
                })
            }
        }
    }

    /// Infallible grow — use when the allocation has already happened and must be tracked.
    pub fn grow(&self, bytes: usize, consumer: &str) {
        if bytes == 0 {
            return;
        }
        let new_used = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(new_used, Ordering::Relaxed);
        log_info!(
            "[{}] +{} bytes for '{}' (used: {}, limit: {})",
            self.name, bytes, consumer, new_used, self.limit.load(Ordering::Relaxed)
        );
    }

    /// Release `bytes` back to the pool.
    pub fn shrink(&self, bytes: usize, consumer: &str) {
        if bytes == 0 {
            return;
        }
        let old = self.used.fetch_sub(bytes, Ordering::Relaxed);
        if old < bytes {
            log_error!(
                "[{}] UNDERFLOW: shrink {} bytes for '{}' but only {} was tracked",
                self.name, bytes, consumer, old
            );
        } else {
            log_info!(
                "[{}] -{} bytes for '{}' (used: {}, limit: {})",
                self.name, bytes, consumer, old - bytes, self.limit.load(Ordering::Relaxed)
            );
        }
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn set_limit(&self, new_limit: usize) {
        let old = self.limit.swap(new_limit, Ordering::Relaxed);
        log_info!("[{}] limit changed: {} -> {}", self.name, old, new_limit);
    }
}

/// RAII handle that tracks a portion of memory reserved from a [`MemoryPool`].
/// Automatically releases all held memory on drop.
pub struct MemoryReservation {
    pool: Arc<MemoryPool>,
    consumer: &'static str,
    size: usize,
}

impl MemoryReservation {
    pub fn new(pool: &Arc<MemoryPool>, consumer: &'static str) -> Self {
        Self {
            pool: Arc::clone(pool),
            consumer,
            size: 0,
        }
    }

    /// Try to grow this reservation. On failure, the reservation is unchanged.
    pub fn try_grow(&mut self, bytes: usize) -> Result<(), PoolExhausted> {
        self.pool.try_grow(bytes, self.consumer)?;
        self.size += bytes;
        Ok(())
    }

    /// Infallible grow.
    pub fn grow(&mut self, bytes: usize) {
        self.pool.grow(bytes, self.consumer);
        self.size += bytes;
    }

    /// Release `bytes` from this reservation.
    pub fn shrink(&mut self, bytes: usize) {
        let actual = bytes.min(self.size);
        self.pool.shrink(actual, self.consumer);
        self.size -= actual;
    }

    /// Resize to a new total. Grows or shrinks as needed.
    pub fn resize(&mut self, new_total: usize) {
        if new_total > self.size {
            self.grow(new_total - self.size);
        } else if new_total < self.size {
            self.shrink(self.size - new_total);
        }
    }

    /// Release all memory back to the pool. Returns bytes freed.
    pub fn free(&mut self) -> usize {
        let s = self.size;
        if s > 0 {
            self.pool.shrink(s, self.consumer);
            self.size = 0;
        }
        s
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn consumer(&self) -> &'static str {
        self.consumer
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.size > 0 {
            log_info!(
                "[{}] reservation '{}' dropped, releasing {} bytes",
                self.pool.name, self.consumer, self.size
            );
            self.pool.shrink(self.size, self.consumer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool(limit: usize) -> Arc<MemoryPool> {
        Arc::new(MemoryPool::new("TEST", limit))
    }

    #[test]
    fn basic_grow_shrink() {
        let pool = test_pool(0); // unlimited
        let mut res = MemoryReservation::new(&pool, "test");
        res.grow(100);
        assert_eq!(res.size(), 100);
        assert_eq!(pool.used(), 100);
        res.shrink(40);
        assert_eq!(res.size(), 60);
        assert_eq!(pool.used(), 60);
        res.free();
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn try_grow_within_limit() {
        let pool = test_pool(1000);
        let mut res = MemoryReservation::new(&pool, "test");
        assert!(res.try_grow(500).is_ok());
        assert!(res.try_grow(400).is_ok());
        assert_eq!(pool.used(), 900);
    }

    #[test]
    fn try_grow_exceeds_limit() {
        let pool = test_pool(1000);
        let mut res = MemoryReservation::new(&pool, "test");
        assert!(res.try_grow(500).is_ok());
        let err = res.try_grow(600).unwrap_err();
        assert_eq!(err.requested, 600);
        assert_eq!(err.used, 500);
        assert_eq!(err.limit, 1000);
        assert_eq!(res.size(), 500); // unchanged
        assert_eq!(pool.used(), 500);
    }

    #[test]
    fn drop_releases_memory() {
        let pool = test_pool(0);
        {
            let mut res = MemoryReservation::new(&pool, "test");
            res.grow(200);
            assert_eq!(pool.used(), 200);
        } // res dropped here
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn resize() {
        let pool = test_pool(0);
        let mut res = MemoryReservation::new(&pool, "test");
        res.resize(100);
        assert_eq!(res.size(), 100);
        assert_eq!(pool.used(), 100);
        res.resize(50);
        assert_eq!(res.size(), 50);
        assert_eq!(pool.used(), 50);
        res.resize(200);
        assert_eq!(res.size(), 200);
        assert_eq!(pool.used(), 200);
    }

    #[test]
    fn multiple_reservations_share_pool() {
        let pool = test_pool(1000);
        let mut r1 = MemoryReservation::new(&pool, "writer1");
        let mut r2 = MemoryReservation::new(&pool, "writer2");
        r1.try_grow(400).unwrap();
        r2.try_grow(400).unwrap();
        assert_eq!(pool.used(), 800);
        // Third allocation that would exceed
        assert!(r2.try_grow(300).is_err());
        assert_eq!(pool.used(), 800);
        drop(r1);
        assert_eq!(pool.used(), 400);
        // Now it fits
        assert!(r2.try_grow(300).is_ok());
        assert_eq!(pool.used(), 700);
    }

    #[test]
    fn peak_tracking() {
        let pool = test_pool(0);
        let mut res = MemoryReservation::new(&pool, "test");
        res.grow(100);
        res.grow(200);
        assert_eq!(pool.peak(), 300);
        res.shrink(250);
        assert_eq!(pool.peak(), 300); // peak unchanged
        assert_eq!(pool.used(), 50);
    }

    #[test]
    fn set_limit_at_runtime() {
        let pool = test_pool(100);
        let mut res = MemoryReservation::new(&pool, "test");
        assert!(res.try_grow(80).is_ok());
        assert!(res.try_grow(30).is_err()); // 80+30 > 100
        pool.set_limit(200);
        assert!(res.try_grow(30).is_ok()); // 80+30 < 200
        assert_eq!(pool.used(), 110);
    }

    #[test]
    fn zero_bytes_is_noop() {
        let pool = test_pool(100);
        let mut res = MemoryReservation::new(&pool, "test");
        assert!(res.try_grow(0).is_ok());
        res.grow(0);
        res.shrink(0);
        assert_eq!(pool.used(), 0);
        assert_eq!(res.size(), 0);
    }
}
