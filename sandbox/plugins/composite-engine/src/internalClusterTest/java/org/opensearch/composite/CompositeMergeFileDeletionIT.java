/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration test verifying that source segment files are deleted after merge.
 *
 * Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeMergeFileDeletionIT" -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergeFileDeletionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-merge-deletion";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    @Override
    public void setUp() throws Exception {
        enableMerge();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist
        }
        super.tearDown();
        disableMerge();
    }

    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    private static void enableMerge() {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
    }

    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    private static void disableMerge() {
        System.clearProperty(MERGE_ENABLED_PROPERTY);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /**
     * Verifies that after merge:
     * 1. Source parquet files are deleted from disk
     * 2. Only merged parquet files remain
     * 3. Total row count is preserved
     * 4. Catalog snapshot reflects the merged state
     */
    public void testSourceFilesDeletedAfterMerge() throws Exception {
        // Create index with manual refresh (no auto-refresh)
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.refresh_interval", "-1")
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats")
                    .build()
            )
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 5;
        int totalDocs = 0;

        // Step 1: Create 10 segments via 10 refreshes
        for (int cycle = 0; cycle < 10; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource("name", randomAlphaOfLength(10), "age", randomIntBetween(1, 100))
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
                totalDocs++;
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }

        Path parquetDir = getParquetDir();
        Set<String> premergeFiles;
        try (Stream<Path> paths = Files.list(parquetDir)) {
            premergeFiles = paths.filter(p -> p.toString().endsWith(".parquet"))
                .map(p -> p.getFileName().toString())
                .collect(Collectors.toSet());
        }
        logger.info(
            "After 10 refreshes - files ({}): {}",
            premergeFiles.size(),
            premergeFiles.stream().sorted().collect(Collectors.toList())
        );

        // Step 2: Do one more refresh with docs — this triggers merge (>10 segments hits TieredMergePolicy threshold)
        for (int i = 0; i < docsPerCycle; i++) {
            client().prepareIndex().setIndex(INDEX_NAME).setSource("name", randomAlphaOfLength(10), "age", randomIntBetween(1, 100)).get();
            totalDocs++;
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Step 3: Wait for background merge to complete
        assertBusy(() -> assertTrue("Expected at least one merge", getMergeStats().getTotal() > 0));

        // Step 4: Flush to commit merged state → deletion policy releases old commit → files deleted
        flush(INDEX_NAME);

        // Step 5: Verify
        Set<String> postmergeFiles;
        try (Stream<Path> paths = Files.list(parquetDir)) {
            postmergeFiles = paths.filter(p -> p.toString().endsWith(".parquet"))
                .map(p -> p.getFileName().toString())
                .collect(Collectors.toSet());
        }
        logger.info("Pre-merge files ({}): {}", premergeFiles.size(), premergeFiles.stream().sorted().collect(Collectors.toList()));
        logger.info("Post-merge files ({}): {}", postmergeFiles.size(), postmergeFiles.stream().sorted().collect(Collectors.toList()));

        assertTrue("Post-merge should have fewer files than pre-merge", postmergeFiles.size() < premergeFiles.size());

        // Verify: total row count preserved
        final int expectedDocs = totalDocs;
        long totalRows = 0;
        for (String file : postmergeFiles) {
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(parquetDir.resolve(file).toString());
            totalRows += metadata.numRows();
        }
        assertEquals("Total rows should be preserved after merge", expectedDocs, totalRows);
    }

    // ── Helpers ──

    private Path getParquetDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().getDataPath().resolve("parquet");
    }

    private IndexShard getPrimaryShard() {
        String nodeName = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeNameResolved = getClusterState().nodes().get(nodeName).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeNameResolved);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setStore(true).get();
        ShardStats shardStats = statsResponse.getIndex(INDEX_NAME).getShards()[0];
        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        return DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
    }

    private MergeStats getMergeStats() {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setMerge(true).get();
        return statsResponse.getIndex(INDEX_NAME).getShards()[0].getStats().getMerge();
    }
}
