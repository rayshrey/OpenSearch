/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * End-to-end proof that a mid-document ingest OOM corrupts a later document that reuses the
 * same VSR row slot.
 *
 * <p>Background: when {@code VSRManager.addDocument} writes a document's fields into the active
 * Arrow {@code VectorSchemaRoot} one vector at a time and an allocation fails partway through,
 * the fields written before the failure remain in their vectors (data + validity bit) even though
 * the row count is never advanced. {@code ParquetWriter.addDoc} catches Arrow's
 * {@link org.apache.arrow.memory.OutOfMemoryException} and returns a recoverable
 * {@code WriteResult.Failure}; the composite then drives {@code rollbackTo(lastGoodRowCount)},
 * which is a no-op against the VSR (the row count never advanced). The writer stays ACTIVE and the
 * same VSR is reused. When the next successful document lands on that same row index but does not
 * overwrite the leaked column, the stale value from the failed document surfaces in the new row.
 *
 * <p>Reproduction (no large payloads required — the OOM is forced purely by a 1-byte pool limit):
 * <ol>
 *   <li><b>doc1</b> {@code {clean:"d1", leak:"warmup"}} — succeeds at row 0. This allocates the
 *       Arrow buffers for the {@code leak} vector (and metadata vectors).</li>
 *   <li>Throttle the ingest pool to 1 byte via the {@code native.allocator.pool.ingest.max}
 *       dynamic setting.</li>
 *   <li><b>doc2</b> {@code {leak:"LEAKED_FROM_DOC2", big:"x"}} — {@code leak} is written at row 1
 *       reusing the already-allocated buffer (no allocation, succeeds under the 1-byte limit); the
 *       first-ever write to the never-allocated {@code big} vector must allocate and throws
 *       {@code OutOfMemoryException}. The doc fails as a per-doc {@code Failure}; the VSR keeps the
 *       stale {@code leak="LEAKED_FROM_DOC2"} at index 1; the row count stays 1.</li>
 *   <li>Reset the ingest pool limit.</li>
 *   <li><b>doc3</b> {@code {clean:"d3"}} — reuses row 1 and does not set {@code leak}.</li>
 *   <li>Flush + refresh, then read the raw Parquet columns.</li>
 * </ol>
 *
 * <p>The row belonging to doc3 (identified by {@code clean == "d3"}) leaks
 * {@code leak == "LEAKED_FROM_DOC2"} instead of {@code null}. The assertion below asserts the
 * corrupted value to <b>prove the bug is present today</b>. Once {@code VSRManager.rollbackTo}
 * (or the OOM catch path) is fixed to scrub the partially-written row, flip the assertion to
 * {@code assertNull(...)} and this becomes the regression guard.
 *
 * <p>The rebalancer is disabled so the ingest pool limit stays exactly where the test sets it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeParquetRollbackCorruptionIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "composite-parquet-rollback-corruption-idx";
    private static final String INGEST_MAX_SETTING = "native.allocator.pool.ingest.max";
    // The leaked column is a FIXED-WIDTH (long) field on purpose: a warm-up doc pre-allocates the
    // vector's capacity for thousands of rows, so a later write to row 1 needs NO allocation and
    // succeeds even under a tight ingest limit — leaving a partial write.
    private static final long LEAKED_VALUE = 999_999_999L;
    private static final long WARMUP_VALUE = 111L;
    // doc2's OOM trigger: a large value so its Arrow buffer allocation is what blows the ingest
    // limit — after `_id` (a small var-width realloc) and `a_leak` (a no-alloc fixed-width write)
    // have already succeeded for this row.
    private static final String BIG_VALUE = "x".repeat(2 * 1024 * 1024);
    // Ingest pool ceiling while doc2 is indexed: above the warm-up shard footprint (~256 KB) so the
    // small `_id` realloc and the pre-allocated `a_leak` write succeed, but below `a_leak` + BIG_VALUE
    // so the large `z_big` allocation OOMs mid-document.
    private static final String INGEST_LIMIT_BYTES = Long.toString(1024 * 1024);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Keep the ingest pool limit fixed at whatever the test sets — no background growth.
            .put("native.allocator.rebalancer.enabled", false)
            .build();
    }

    public void testMidDocOomLeaksStaleColumnIntoReusedRow() throws Exception {
        createCorruptionIndex();

        // doc1 — succeeds at row 0. Writing `a_leak` (a fixed-width long) here allocates that
        // vector with capacity for thousands of rows, so a later write to row 1 reuses it with no
        // allocation (and thus without tripping the pool limit).
        IndexResponse doc1 = client().prepareIndex(INDEX_NAME).setSource("clean", "d1", "a_leak", WARMUP_VALUE).get();
        assertEquals(RestStatus.CREATED, doc1.status());

        // Throttle the ingest Arrow pool so doc2 fails mid-write: small allocations (the `_id`
        // var-width realloc) and the pre-allocated `a_leak` write still succeed, but the large
        // `z_big` allocation blows the limit.
        setIngestPoolLimit(INGEST_LIMIT_BYTES);
        String doc2Failure;
        try {
            // doc2 — fields are processed in order: `_id` (small realloc, fits), `a_leak`
            // (pre-allocated fixed-width, no allocation -> written at row 1), then `z_big` (a large
            // value whose allocation exceeds the limit -> OOM). The row count never advances, so the
            // written `a_leak` is a stale partial write.
            BulkResponse bulk = client().prepareBulk()
                .add(client().prepareIndex(INDEX_NAME).setSource("a_leak", LEAKED_VALUE, "z_big", BIG_VALUE))
                .get();
            assertEquals(1, bulk.getItems().length);
            doc2Failure = bulk.getItems()[0].getFailureMessage();
            assertTrue("doc2 must fail with a mid-document ingest OOM: " + doc2Failure, bulk.getItems()[0].isFailed());
        } finally {
            // Relieve the limit so doc3, flush and teardown can allocate freely.
            resetIngestPoolLimit();
        }
        logger.info("[corruption-it] doc2 failure message: {}", doc2Failure);

        // Engine must still be open — an ingest OOM is a recoverable per-doc failure, not a tragic event.
        assertNotNull("engine must survive a per-doc ingest OOM", getEngine(INDEX_NAME).commitStats());

        // doc3 — reuses row 1 (the row doc2 partially wrote). It sets `clean` but NOT `a_leak`, so a
        // correct implementation would leave `a_leak` null for this row.
        IndexResponse doc3 = client().prepareIndex(INDEX_NAME).setSource("clean", "d3").get();
        assertEquals(RestStatus.CREATED, doc3.status());

        flushIndex(INDEX_NAME);
        refreshIndex(INDEX_NAME);

        List<Map<String, Object>> rows = readParquetRows(INDEX_NAME);
        logger.info("[corruption-it] parquet rows ({}): {}", rows.size(), rows);

        Map<String, Object> doc3Row = rows.stream()
            .filter(r -> "d3".equals(r.get("clean")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find doc3's row (clean=d3) in parquet output: " + rows));

        // CORRUPTION: doc3 never set `a_leak`, yet the row carries the value from the failed doc2 that
        // partially wrote this same slot before the OOM.
        //
        // Correct behaviour (after the VSR-rollback fix) is `assertNull(doc3Row.get("a_leak"))` — flip
        // this assertion then to turn this into a regression guard.
        Object leaked = doc3Row.get("a_leak");
        assertNotNull("doc3's row leaked a_leak from the OOM-failed doc2 (proves data corruption)", leaked);
        assertEquals(
            "doc3's row must leak the value partially written by the OOM-failed doc2 (proves data corruption)",
            LEAKED_VALUE,
            ((Number) leaked).longValue()
        );
    }

    private void createCorruptionIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(settings)
            .setMapping("clean", "type=keyword", "a_leak", "type=long", "z_big", "type=keyword")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void setIngestPoolLimit(String bytes) {
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(INGEST_MAX_SETTING, bytes))
                .get()
                .isAcknowledged()
        );
    }

    private void resetIngestPoolLimit() {
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(INGEST_MAX_SETTING))
                .get()
                .isAcknowledged()
        );
    }

    /** Reads every row of every parquet file backing the primary shard as a list of column maps. */
    private List<Map<String, Object>> readParquetRows(String indexName) throws IOException {
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> allRows = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot()) {
            for (Segment segment : snapshot.get().getSegments()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
                if (wfs == null) {
                    continue;
                }
                for (String file : wfs.files()) {
                    allRows.addAll(parseJsonRows(RustBridge.readAsJson(parquetDir.resolve(file).toString())));
                }
            }
        }
        return allRows;
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> parseJsonRows(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(o -> (Map<String, Object>) o).collect(Collectors.toList());
        }
    }
}
