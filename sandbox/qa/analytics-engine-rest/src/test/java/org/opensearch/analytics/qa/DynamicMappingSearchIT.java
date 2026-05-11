/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end integration test for dynamic mapping with search verification.
 * <p>
 * Validates that documents with dynamically added fields (not in the original mapping)
 * are correctly indexed into a composite parquet index AND are searchable via PPL.
 * <p>
 * Tests schema evolution across writer generations: fields added in later phases
 * are queryable, while earlier docs correctly return null for those fields.
 * <p>
 * Run with:
 * ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.DynamicMappingSearchIT" -Dsandbox.enabled=true
 */
public class DynamicMappingSearchIT extends AnalyticsRestTestCase {

    private static final String INDEX = "dynamic_mapping_search_e2e";

    /**
     * Full end-to-end test: 3-phase ingestion with progressive schema evolution,
     * verifying search works correctly at each stage.
     *
     * Phase 1: docs with initial schema (name, age)
     * Phase 2: docs with new dynamic fields (city, score)
     * Phase 3: docs with another new field (priority) after flush
     */
    public void testSearchOnDynamicallyAddedFields() throws Exception {
        createIndex();

        // ── Phase 1: Initial schema ─────────────────────────────────────────
        bulkIndex(
            "{\"index\": {}}\n{\"name\": \"alice\", \"age\": 30}\n"
                + "{\"index\": {}}\n{\"name\": \"bob\", \"age\": 25}\n"
                + "{\"index\": {}}\n{\"name\": \"carol\", \"age\": 35}\n"
                + "{\"index\": {}}\n{\"name\": \"dave\", \"age\": 28}\n"
                + "{\"index\": {}}\n{\"name\": \"eve\", \"age\": 32}\n"
        );
        flush();

        assertPplCount("source = " + INDEX + " | stats count() as cnt", 5);

        // ── Phase 2: Dynamic fields (city, points) ─────────────────────────
        // Using integer for dynamic numeric field to avoid float/double type mismatch
        // between OpenSearch dynamic mapping (float) and Substrait schema (double).
        bulkIndex(
            "{\"index\": {}}\n{\"name\": \"frank\", \"age\": 40, \"city\": \"seattle\", \"points\": 95}\n"
                + "{\"index\": {}}\n{\"name\": \"grace\", \"age\": 22, \"city\": \"portland\", \"points\": 88}\n"
                + "{\"index\": {}}\n{\"name\": \"hank\", \"age\": 45, \"city\": \"seattle\", \"points\": 72}\n"
                + "{\"index\": {}}\n{\"name\": \"iris\", \"age\": 29, \"city\": \"portland\", \"points\": 91}\n"
                + "{\"index\": {}}\n{\"name\": \"jack\", \"age\": 33, \"city\": \"seattle\", \"points\": 85}\n"
        );
        flush();

        // Verify mapping updated
        assertMappingContains("city", "points");

        // All 10 docs visible
        assertPplCount("source = " + INDEX + " | stats count() as cnt", 10);

        // Filter on dynamic keyword field
        assertPplCount("source = " + INDEX + " | where city = 'seattle' | stats count() as cnt", 3);

        // Filter on dynamic numeric field
        assertPplCount("source = " + INDEX + " | where points >= 90 | stats count() as cnt", 2);

        // Aggregation on dynamic field (phase 1 docs have null points — should be excluded from sum)
        assertPplValue("source = " + INDEX + " | stats sum(points) as total", "total", 431.0);

        // Null check: phase 1 docs don't have city
        assertPplCount("source = " + INDEX + " | where isnull(city) | stats count() as cnt", 5);

        // Range on original field still works across all docs
        assertPplCount("source = " + INDEX + " | where age > 30 | stats count() as cnt", 5);

        // ── Phase 3: Another new field (priority) after flush ────────────────
        bulkIndex(
            "{\"index\": {}}\n{\"name\": \"kate\", \"age\": 27, \"city\": \"seattle\", \"points\": 90, \"priority\": 1}\n"
                + "{\"index\": {}}\n{\"name\": \"leo\", \"age\": 38, \"city\": \"portland\", \"points\": 78, \"priority\": 2}\n"
                + "{\"index\": {}}\n{\"name\": \"mia\", \"age\": 31, \"city\": \"seattle\", \"points\": 94, \"priority\": 1}\n"
        );
        flush();

        // Verify mapping updated
        assertMappingContains("priority");

        // All 13 docs visible
        assertPplCount("source = " + INDEX + " | stats count() as cnt", 13);

        // Only phase 3 docs have priority
        assertPplCount("source = " + INDEX + " | where isnotnull(priority) | stats count() as cnt", 3);

        // Filter on latest-generation field
        assertPplCount("source = " + INDEX + " | where priority = 1 | stats count() as cnt", 2);

        // city filter now spans phase 2 + phase 3
        assertPplCount("source = " + INDEX + " | where city = 'seattle' | stats count() as cnt", 5);

        // Aggregation across all generations
        assertPplValue("source = " + INDEX + " | stats sum(points) as total", "total", 693.0);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"age\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void bulkIndex(String ndjson) throws Exception {
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.setJsonEntity(ndjson);
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
    }

    private void flush() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    private Map<String, Object> executePPL(String ppl) throws IOException {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(req);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    private void assertPplCount(String ppl, int expected) throws IOException {
        Map<String, Object> result = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("Expected 1 row for count query: " + ppl, 1, rows.size());
        long actual = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("Count mismatch for: " + ppl, expected, actual);
    }

    private void assertPplValue(String ppl, String column, double expected) throws IOException {
        Map<String, Object> result = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals(1, rows.size());
        int idx = columns.indexOf(column);
        assertTrue("Column '" + column + "' not found in: " + columns, idx >= 0);
        double actual = ((Number) rows.get(0).get(idx)).doubleValue();
        assertEquals("Value mismatch for: " + ppl, expected, actual, 0.01);
    }

    @SuppressWarnings("unchecked")
    private void assertMappingContains(String... fields) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_mapping"));
        Map<String, Object> map = assertOkAndParse(response, "Get mapping");
        Map<String, Object> indexMap = (Map<String, Object>) map.get(INDEX);
        Map<String, Object> mappings = (Map<String, Object>) indexMap.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        for (String field : fields) {
            assertTrue("Mapping should contain field '" + field + "'", properties.containsKey(field));
        }
    }
}
