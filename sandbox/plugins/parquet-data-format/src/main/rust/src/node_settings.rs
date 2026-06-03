/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Node-level settings pushed from Java via FFM.
//! Values are read by merge/write paths when creating new operations.

use std::sync::RwLock;

struct NodeSettings {
    merge_io_rate_mb_per_sec: f64,
}

static NODE_SETTINGS: RwLock<NodeSettings> = RwLock::new(NodeSettings {
    merge_io_rate_mb_per_sec: 20.0,
});

/// Returns the current merge IO rate in MB/sec.
pub fn get_merge_io_rate() -> f64 {
    NODE_SETTINGS.read().unwrap().merge_io_rate_mb_per_sec
}

/// Sets the merge IO rate. Called from Java via FFM.
pub fn set_merge_io_rate(value: f64) {
    NODE_SETTINGS.write().unwrap().merge_io_rate_mb_per_sec = value;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_merge_io_rate() {
        set_merge_io_rate(42.5);
        assert!((get_merge_io_rate() - 42.5).abs() < f64::EPSILON);
        // Reset to default
        set_merge_io_rate(20.0);
    }
}
