/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Verifies end-to-end Java → Rust propagation of the merge IO rate setting.
 */
public class MergeIORateTests extends OpenSearchTestCase {

    public void testSetAndGetMergeIORate() {
        // Default should be 20.0
        double defaultRate = RustBridge.getMergeIORate();
        assertEquals(20.0, defaultRate, 0.001);

        // Set a new value and verify it round-trips through Rust
        RustBridge.setMergeIORate(42.5);
        assertEquals(42.5, RustBridge.getMergeIORate(), 0.001);

        // Set another value
        RustBridge.setMergeIORate(5.0);
        assertEquals(5.0, RustBridge.getMergeIORate(), 0.001);

        // Reset to default
        RustBridge.setMergeIORate(20.0);
        assertEquals(20.0, RustBridge.getMergeIORate(), 0.001);
    }
}
