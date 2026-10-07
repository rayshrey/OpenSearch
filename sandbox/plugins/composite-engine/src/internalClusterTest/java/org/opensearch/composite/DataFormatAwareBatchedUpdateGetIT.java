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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
}
