/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;

/**
 * IT verifying that dynamic cluster setting updates for merge IO rate
 * propagate end-to-end from Java to the Rust native layer.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 1, maxNumDataNodes = 1)
public class MergeIORateSettingIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    public void testDynamicMergeIORateSettingPropagesToRust() {
        // Verify default
        assertThat("default merge IO rate should be 20.0", RustBridge.getMergeIORate(), closeTo(20.0, 0.001));

        // Update setting dynamically
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(ParquetSettings.MERGE_IO_RATE_SETTING.getKey(), 7.5))
                .get()
        );

        // Verify Rust side received the new value
        assertThat("merge IO rate should update to 7.5", RustBridge.getMergeIORate(), closeTo(7.5, 0.001));

        // Update again
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(ParquetSettings.MERGE_IO_RATE_SETTING.getKey(), 100.0))
                .get()
        );

        assertThat("merge IO rate should update to 100.0", RustBridge.getMergeIORate(), closeTo(100.0, 0.001));
    }
}
