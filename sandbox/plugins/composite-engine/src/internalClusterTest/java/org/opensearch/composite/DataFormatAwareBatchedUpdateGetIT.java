/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatAwareEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end coverage for the batched primary-store read behind bulk partial updates.
 *
 * <p>An update's first leg is a get of the current document, needed to merge the partial doc over
 * it. On a composite index whose documents have left the version map, that get reads a row from the
 * primary store, and the bulk execution loop would otherwise pay that read once per item. The
 * prefetch declares every update id in the request up front, so ids sharing a file are read in one
 * pass.
 *
 * <p>These tests pin the behaviour that must hold whether or not the prefetch fires: the merged
 * documents. The optimization is only allowed to change how the rows were read, never what they
 * contain — so each test asserts the full post-update document, including the fields the partial
 * update did not mention, which is exactly what a wrong or stale prefetched row would corrupt.
 */
public class DataFormatAwareBatchedUpdateGetIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "dfae_batched_update";

    /**
     * Composite index with auto-refresh disabled, so a refresh can be placed deliberately and the
     * updates are guaranteed to resolve through the primary-store read path rather than the
     * version map.
     */
    private void createIndex(int shards) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .put(IndexSettings.BATCHED_UPDATE_GET_ENABLED_SETTING.getKey(), true)
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer", "note", "type=keyword")
            .get();
        ensureGreen(INDEX);
    }

    /** Seeds {@code count} docs and refreshes, so every one is reachable only via the stored row. */
    private void seedAndRefresh(int count) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < count; i++) {
            bulk.add(client().prepareIndex(INDEX).setId(id(i)).setSource("name", "orig-" + i, "value", i, "note", "note-" + i));
        }
        assertNoBulkFailures(bulk.get());
        client().admin().indices().prepareRefresh(INDEX).get();
    }

    private static void assertNoBulkFailures(BulkResponse response) {
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    /**
     * Asserts the read-ahead actually ran and was consumed, so a silent fallback cannot pass for
     * the optimization. Every failure inside {@code prefetchUpdateGets} is caught and logged at
     * debug, by design: the per-document get still produces the right document, so correctness
     * assertions alone stay green while the batched path is disabled or broken. Only these counters
     * separate "the batch read the rows" from "every document was read one at a time".
     */
    private void assertPrefetchServedUpdates(String indexName, int minUpdates) {
        DataFormatAwareEngine engine = getEngine(indexName);
        assertThat("rows must be read ahead for the bulk's updates", engine.prefetchedDocCount(), greaterThanOrEqualTo((long) minUpdates));
        assertThat(
            "the reads must be served from the read-ahead cache",
            engine.prefetchHitCount(),
            greaterThanOrEqualTo((long) minUpdates)
        );
    }

    private static String id(int i) {
        return "doc-" + i;
    }

    private GetResponse get(String id) {
        return client().prepareGet(INDEX, id).get();
    }

    private static int value(GetResponse r) {
        return ((Number) r.getSourceAsMap().get("value")).intValue();
    }

    /**
     * The core case. Many partial updates in one bulk request, every document already flushed to
     * the primary store, so each update's get would otherwise be its own row read. Each update
     * touches only {@code value}; {@code name} and {@code note} must survive untouched, which only
     * holds if the row merged into each update was that document's own current row.
     */
    public void testBulkPartialUpdatesMergeAgainstTheCorrectRows() throws Exception {
        createIndex(1);
        final int docs = 50;
        seedAndRefresh(docs);

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            bulk.add(client().prepareUpdate(INDEX, id(i)).setDoc("value", 1000 + i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        for (int i = 0; i < docs; i++) {
            GetResponse got = get(id(i));
            assertTrue("doc " + id(i) + " must exist after update", got.isExists());
            Map<String, Object> source = got.getSourceAsMap();
            assertEquals("updated field", 1000 + i, value(got));
            // The fields the partial update omitted. A stale or cross-wired row shows up here.
            assertEquals("untouched name for " + id(i), "orig-" + i, source.get("name"));
            assertEquals("untouched note for " + id(i), "note-" + i, source.get("note"));
            assertEquals("version after one update", 2L, got.getVersion());
        }
        assertPrefetchServedUpdates(INDEX, docs);
    }

    /**
     * Documents spread over several generations, so one bulk request's updates resolve to rows in
     * different primary-store files. Exercises the grouping — a batched read is issued per file,
     * and rows must be matched back to the right document across all of them.
     */
    public void testBulkPartialUpdatesAcrossMultipleGenerations() throws Exception {
        createIndex(1);
        final int perGeneration = 10;
        final int generations = 4;
        for (int g = 0; g < generations; g++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < perGeneration; i++) {
                int n = g * perGeneration + i;
                bulk.add(client().prepareIndex(INDEX).setId(id(n)).setSource("name", "orig-" + n, "value", n, "note", "note-" + n));
            }
            assertNoBulkFailures(bulk.get());
            // Each refresh seals a generation, so the next batch lands in a new file.
            client().admin().indices().prepareRefresh(INDEX).get();
        }

        final int total = perGeneration * generations;
        BulkRequestBuilder updates = client().prepareBulk();
        for (int n = 0; n < total; n++) {
            updates.add(client().prepareUpdate(INDEX, id(n)).setDoc("value", 2000 + n));
        }
        assertNoBulkFailures(updates.get());

        for (int n = 0; n < total; n++) {
            GetResponse got = get(id(n));
            assertEquals("updated field for " + id(n), 2000 + n, value(got));
            assertEquals("untouched name for " + id(n), "orig-" + n, got.getSourceAsMap().get("name"));
            assertEquals("untouched note for " + id(n), "note-" + n, got.getSourceAsMap().get("note"));
        }
    }

    /**
     * Two updates to the same id in one bulk request. The second must observe the first — it cannot
     * be served a row read before either ran. This is the case the version-map-first ordering
     * protects: once the first update writes, the id is live in the version map, which takes
     * precedence over anything read ahead.
     */
    public void testRepeatedUpdatesToSameIdWithinOneBulkSeeEachOther() throws Exception {
        createIndex(1);
        seedAndRefresh(3);

        BulkRequestBuilder bulk = client().prepareBulk();
        // doc-0 twice: +5 then a rename. The rename must land on top of the first update's doc.
        bulk.add(client().prepareUpdate(INDEX, id(0)).setDoc("value", 5));
        bulk.add(client().prepareUpdate(INDEX, id(0)).setDoc("name", "renamed"));
        bulk.add(client().prepareUpdate(INDEX, id(1)).setDoc("value", 11));
        assertNoBulkFailures(bulk.get());

        GetResponse doc0 = get(id(0));
        assertEquals("second update must have been applied", "renamed", doc0.getSourceAsMap().get("name"));
        assertEquals("first update must not have been lost", 5, value(doc0));
        assertEquals("note must survive both partial updates", "note-0", doc0.getSourceAsMap().get("note"));
        assertEquals("two updates to the same id", 3L, doc0.getVersion());

        assertEquals(11, value(get(id(1))));
        assertEquals("untouched doc", 2, value(get(id(2))));
    }

    /**
     * A bulk request mixing updates with indexes and deletes, including an update to a document
     * that does not exist. The prefetch sees ids it cannot serve and ids that are not updates at
     * all; neither may disturb the outcome.
     */
    public void testMixedBulkWithMissingAndNonUpdateItems() throws Exception {
        createIndex(1);
        seedAndRefresh(5);

        BulkRequestBuilder bulk = client().prepareBulk();
        bulk.add(client().prepareUpdate(INDEX, id(0)).setDoc("value", 100));
        bulk.add(client().prepareIndex(INDEX).setId("fresh").setSource("name", "fresh", "value", 7, "note", "n"));
        bulk.add(client().prepareUpdate(INDEX, "absent").setDoc("value", 1));
        bulk.add(client().prepareDelete(INDEX, id(4)));
        bulk.add(client().prepareUpdate(INDEX, id(1)).setDoc("value", 101));
        BulkResponse response = bulk.get();

        // Only the update to the missing document may fail.
        List<String> unexpected = new ArrayList<>();
        for (var item : response.getItems()) {
            if (item.isFailed() && "absent".equals(item.getId()) == false) {
                unexpected.add(item.getId() + ": " + item.getFailureMessage());
            }
        }
        assertTrue("only the missing-doc update may fail, got " + unexpected, unexpected.isEmpty());

        assertEquals(100, value(get(id(0))));
        assertEquals("untouched name", "orig-0", get(id(0)).getSourceAsMap().get("name"));
        assertEquals(101, value(get(id(1))));
        assertEquals(7, value(get("fresh")));
        assertFalse("deleted doc must be gone", get(id(4)).isExists());
        assertFalse("update of a missing doc must not create it", get("absent").isExists());
    }

    /**
     * Updates issued without an intervening refresh, so the documents are still live in the version
     * map. The realtime path must serve them and the result must be identical — nothing read ahead
     * may shadow a version-map entry.
     */
    public void testBulkPartialUpdatesWithinRefreshWindow() throws Exception {
        createIndex(1);
        final int docs = 20;
        BulkRequestBuilder seed = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            seed.add(client().prepareIndex(INDEX).setId(id(i)).setSource("name", "orig-" + i, "value", i, "note", "note-" + i));
        }
        assertNoBulkFailures(seed.get());
        // No refresh: every doc is still in the version map.

        BulkRequestBuilder updates = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            updates.add(client().prepareUpdate(INDEX, id(i)).setDoc("value", 300 + i));
        }
        assertNoBulkFailures(updates.get());

        for (int i = 0; i < docs; i++) {
            GetResponse got = get(id(i));
            assertEquals(300 + i, value(got));
            assertEquals("orig-" + i, got.getSourceAsMap().get("name"));
            assertEquals("note-" + i, got.getSourceAsMap().get("note"));
        }
    }

    /**
     * Updates surviving a refresh that lands between two bulk requests. The second request's
     * prefetch must not reuse anything read before that refresh.
     */
    public void testBulkPartialUpdatesSurviveRefreshBetweenRequests() throws Exception {
        createIndex(1);
        final int docs = 15;
        seedAndRefresh(docs);

        BulkRequestBuilder first = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            first.add(client().prepareUpdate(INDEX, id(i)).setDoc("value", 400 + i));
        }
        assertNoBulkFailures(first.get());
        client().admin().indices().prepareRefresh(INDEX).get();

        BulkRequestBuilder second = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            second.add(client().prepareUpdate(INDEX, id(i)).setDoc("note", "second-" + i));
        }
        assertNoBulkFailures(second.get());

        for (int i = 0; i < docs; i++) {
            GetResponse got = get(id(i));
            assertEquals("second request's field", "second-" + i, got.getSourceAsMap().get("note"));
            // Must reflect the FIRST request, not the pre-refresh seed value.
            assertEquals("first request's field must survive", 400 + i, value(got));
            assertEquals("orig-" + i, got.getSourceAsMap().get("name"));
            assertEquals("two sequential updates", 3L, got.getVersion());
        }
    }

    /**
     * Concurrent bulk updates contending on the same rows must produce correctly merged documents.
     *
     * <p>This started as an attempt to exercise the one window the prefetch design leaves open: in
     * {@code getById} the per-uid version-map lock is released after the miss and before the
     * prefetch cache is polled, so a concurrent write can land in between and leave the cached row
     * stale. The generation counter does not catch that — only a refresh bumps it, not an ordinary
     * write — and the backstop is that {@code UpdateHelper} stamps the rebuilt index request with
     * the get's {@code ifSeqNo}, so a stale read turns into a version conflict rather than a lost
     * update.
     *
     * <p>That backstop could not be provoked here. With retries disabled and contention pushed to
     * 24 threads x 2 documents x 15 rounds — 720 updates all competing for two rows — the run
     * produced <em>zero</em> version conflicts, so the get-then-index sequence never actually
     * interleaved on a document. This test therefore does <em>not</em> cover the conflict path; it
     * is a regression guard on the thing that is reachable, namely that concurrent prefetch-served
     * merges stay correct.
     *
     * <p>Each thread owns one field and writes it to every document, so the threads are disjoint in
     * what they set but fully overlapping in what they read. Merging a partial doc over a stale or
     * cross-wired base row drops whichever peer fields that row predates, so any such failure shows
     * up directly as a missing field below.
     */
    public void testConcurrentBulkUpdatesNeverMergeAgainstAStaleRow() throws Exception {
        // Few documents, many threads, several rounds each, so every request contends for rows
        // another thread is concurrently updating. Retries are kept because a conflict is the
        // correct outcome of a race, even though none were observed here (see the javadoc).
        final int threads = 12;
        final int docs = 5;
        final int rounds = 8;
        createIndexWithThreadFields(1, threads);
        seedAndRefresh(docs);

        final CyclicBarrier startTogether = new CyclicBarrier(threads);
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        final List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            Thread worker = new Thread(() -> {
                try {
                    startTogether.await(30, TimeUnit.SECONDS);
                    for (int round = 0; round < rounds; round++) {
                        // More than one update id per request, so the prefetch pre-pass fires.
                        BulkRequestBuilder bulk = client().prepareBulk();
                        for (int i = 0; i < docs; i++) {
                            bulk.add(
                                client().prepareUpdate(INDEX, id(i))
                                    .setDoc(threadField(threadId), threadId)
                                    // A conflict is the correct outcome of a racing write; the
                                    // retry re-reads and re-merges. What must never happen is a
                                    // silent merge against a stale row.
                                    .setRetryOnConflict(threads * rounds)
                            );
                        }
                        assertNoBulkFailures(bulk.get());
                    }
                } catch (Throwable e) {
                    failures.add(e);
                }
            }, "batched-update-racer-" + t);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join(TimeUnit.MINUTES.toMillis(3));
            assertFalse("worker " + worker.getName() + " did not finish", worker.isAlive());
        }
        assertTrue("no worker may fail: " + failures, failures.isEmpty());

        client().admin().indices().prepareRefresh(INDEX).get();
        for (int i = 0; i < docs; i++) {
            GetResponse got = get(id(i));
            assertTrue("doc " + id(i) + " must exist", got.isExists());
            Map<String, Object> source = got.getSourceAsMap();
            // Every thread's field must have survived. Had any thread's final write merged over a
            // stale base row, whichever peer field that row predates would be missing here.
            for (int t = 0; t < threads; t++) {
                assertEquals(
                    "lost update on " + id(i) + " field " + threadField(t) + " (merged against a stale row): " + source,
                    t,
                    ((Number) source.get(threadField(t))).intValue()
                );
            }
            // And the fields nobody touched are still intact.
            assertEquals("untouched name for " + id(i), "orig-" + i, source.get("name"));
            assertEquals("untouched note for " + id(i), "note-" + i, source.get("note"));
            assertEquals("untouched value for " + id(i), i, value(got));
        }
    }

    /**
     * Updates whose rows are scattered across many row groups and data pages of ONE file. The row
     * group and page geometry is forced small so a single flushed generation holds tens of row
     * groups, which is the shape a batched read has to map positions through: ids are resolved to a
     * row group and an offset inside it, row groups holding no requested row are never opened, and
     * the rows must still come back matched to their own documents.
     *
     * <p>The batch is also well past the 20-entry limit below which a predicate over the row-id
     * column could be expanded into prunable comparisons, so it covers the size at which a
     * predicate-based read degenerates into a full-file scan.
     */
    public void testBulkPartialUpdatesSpreadAcrossRowGroupsOfOneFile() throws Exception {
        final int docs = 1_000;
        final int rowGroupRows = 100;
        createIndexWithRowGroupGeometry(1, rowGroupRows, 20);
        seedAndRefresh(docs);

        // One update per row group, so every row group is touched but only a single row in each.
        List<Integer> updated = new ArrayList<>();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 7; i < docs; i += rowGroupRows) {
            updated.add(i);
            bulk.add(client().prepareUpdate(INDEX, id(i)).setDoc("value", 2000 + i));
        }
        assertEquals("one update per row group", docs / rowGroupRows, updated.size());
        assertNoBulkFailures(bulk.get());

        for (int i : updated) {
            GetResponse got = get(id(i));
            assertTrue("doc " + id(i) + " must exist after update", got.isExists());
            Map<String, Object> source = got.getSourceAsMap();
            assertEquals("updated field for " + id(i), 2000 + i, value(got));
            assertEquals("untouched name for " + id(i), "orig-" + i, source.get("name"));
            assertEquals("untouched note for " + id(i), "note-" + i, source.get("note"));
        }
        // Documents the batch did not name must be untouched, which a misread position would break.
        for (int i : List.of(0, 6, 8, rowGroupRows - 1, docs / 2, docs - 1)) {
            if (updated.contains(i)) {
                continue;
            }
            GetResponse got = get(id(i));
            assertEquals("unrequested doc " + id(i) + " must keep its seeded value", i, value(got));
            assertEquals("version of an unrequested doc", 1L, got.getVersion());
        }
        assertPrefetchServedUpdates(INDEX, updated.size());
    }

    /**
     * As {@link #createIndex(int)}, but with the parquet row group and data page sizes forced small
     * so a modest seed produces many row groups and pages in one file.
     */
    private void createIndexWithRowGroupGeometry(int shards, int rowGroupRows, int pageRows) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .put(IndexSettings.BATCHED_UPDATE_GET_ENABLED_SETTING.getKey(), true)
            .put("index.parquet.row_group_max_rows", rowGroupRows)
            .put("index.parquet.page_row_limit", pageRows)
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer", "note", "type=keyword")
            .get();
        ensureGreen(INDEX);
    }

    private static String threadField(int threadId) {
        return "t" + threadId;
    }

    /** As {@link #createIndex(int)}, plus one pre-declared integer field per racing thread. */
    private void createIndexWithThreadFields(int shards, int threads) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .put(IndexSettings.BATCHED_UPDATE_GET_ENABLED_SETTING.getKey(), true)
            .build();
        List<String> mapping = new ArrayList<>(List.of("name", "type=keyword", "value", "type=integer", "note", "type=keyword"));
        for (int t = 0; t < threads; t++) {
            mapping.add(threadField(t));
            mapping.add("type=integer");
        }
        client().admin().indices().prepareCreate(INDEX).setSettings(settings).setMapping(mapping.toArray(new String[0])).get();
        ensureGreen(INDEX);
    }
}
