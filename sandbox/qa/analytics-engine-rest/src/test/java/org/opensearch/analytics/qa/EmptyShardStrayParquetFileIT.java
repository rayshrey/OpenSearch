/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * PPL results must not include rows from parquet files that are on disk but not in the shard's catalog
 * snapshot, when the shard's catalog snapshot is empty.
 *
 * <p>The replica of a shard whose docs were all deleted is in that state until its next commit: the
 * catalog is empty but the dropped generation's parquet file is still on disk. An empty shard used to
 * register a {@code ListingTable} whose (empty) file list DataFusion's list-files cache does not store,
 * so DataFusion listed the shard directory and multi-shard plans scanned the stray file without
 * deleted-doc masking. The state is reproduced here on primaries by copying another shard's parquet
 * file into the emptied shard's parquet directory (test JVM and test cluster share the filesystem).
 *
 * <p>Each query is compared against the same query on a reference index with the same shard layout and
 * the same live documents but no stray file, so the stray file is the only difference.
 */
public class EmptyShardStrayParquetFileIT extends AnalyticsRestTestCase {

    private static final int SHARDS = 3;
    private static final int DOCS = 300;
    private static final String MAPPING = "\"properties\": {\"k\": {\"type\": \"keyword\"}, \"v\": {\"type\": \"long\"}, \"name\": {\"type\": \"keyword\"}}";
    private static final List<String> QUERIES = List.of(
        "stats count() as c",
        "where v < 150 | stats count() as c",
        "where k = 'a' | stats count() as c",
        "stats count() as c by k | sort k",
        "stats sum(v) as total, avg(v) as mean, dc(k) as keys, min(name) as lo, max(name) as hi",
        "where v >= 250 | sort v | head 5 | fields v"
    );

    public void testEmptyShardWithStrayParquetFile() throws IOException {
        runScenario("empty_shard_stray", false);
    }

    public void testAllShardsEmptyWithStrayParquetFile() throws IOException {
        runScenario("empty_shard_all", true);
    }

    /** Single-shard index with every doc deleted (and a stray file): aggregates must still return their one row. */
    public void testSingleShardEmptyWithStrayParquetFile() throws IOException {
        runScenario("empty_shard_single", 1, true);
    }

    /** Never-ingested indices (no parquet files or directory at all, the #21754 empty-index case): 1 shard must match 3 shards. */
    public void testNeverIngestedIndex() throws IOException {
        createCompositeIndex("empty_never_1", 1);
        createCompositeIndex("empty_never_3", SHARDS);
        for (String q : QUERIES) {
            assertEquals(q, executePpl("source = empty_never_3 | " + q).get("datarows"), executePpl("source = empty_never_1 | " + q).get("datarows"));
        }
        assertEquals(List.of(List.of(0)), executePpl("source = empty_never_1 | where v < 150 | stats count() as c").get("datarows"));
    }

    /** Empties shard 0 (or every shard), places a stray parquet file in shard 0, compares all queries. */
    private void runScenario(String index, boolean emptyAllShards) throws IOException {
        runScenario(index, SHARDS, emptyAllShards);
    }

    /**
     * Same, for a test index with {@code shards} shards. The reference index always has {@link #SHARDS} shards; with
     * fewer test shards only the all-empty scenario is meaningful (the reference then holds no docs either).
     */
    private void runScenario(String index, int shards, boolean emptyAllShards) throws IOException {
        assertTrue("fewer shards than the reference only for the all-empty scenario", shards == SHARDS || emptyAllShards);
        String reference = index + "_ref";
        createCompositeIndex(index, shards);
        createCompositeIndex(reference, SHARDS);
        bulk(index, IntStream.range(0, DOCS).mapToObj(i -> indexAction(index, i)).collect(Collectors.joining()));
        Path stray = Files.createTempFile("stray", ".parquet");
        Files.copy(parquetFiles(parquetDir(index, shards > 1 ? 1 : 0)).get(0), stray, StandardCopyOption.REPLACE_EXISTING);

        StringBuilder deletes = new StringBuilder();
        StringBuilder live = new StringBuilder();
        for (int i = 0; i < DOCS; i++) {
            if (emptyAllShards || shardOf(index, "d" + i) == 0) {
                deletes.append("{\"delete\": {\"_index\": \"").append(index).append("\", \"_id\": \"d").append(i).append("\"}}\n");
            } else {
                live.append(indexAction(reference, i));
            }
        }
        bulk(index, deletes.toString());
        if (live.length() > 0) {
            bulk(reference, live.toString());
        }
        Path dir = parquetDir(index, 0);
        Files.createDirectories(dir);
        Files.copy(stray, dir.resolve("_parquet_file_generation_999999.parquet"), StandardCopyOption.REPLACE_EXISTING);

        for (String q : QUERIES) {
            assertEquals(q, executePpl("source = " + reference + " | " + q).get("datarows"), executePpl("source = " + index + " | " + q).get("datarows"));
        }
    }

    private static void createCompositeIndex(String index, int shards) throws IOException {
        try {
            deleteIndex(index);
        } catch (Exception ignored) {}
        Settings settings = Settings.builder()
            .put("number_of_shards", shards)
            .put("number_of_replicas", 0)
            .put("refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.append_only.enabled", false)
            .build();
        createIndex(index, settings, MAPPING);
        ensureGreen(index);
    }

    private static String indexAction(String index, int i) {
        return String.format(
            Locale.ROOT,
            "{\"index\": {\"_index\": \"%s\", \"_id\": \"d%d\"}}\n{\"k\": \"%s\", \"v\": %d, \"name\": \"n%05d\"}\n",
            index,
            i,
            "abc".charAt(i % 3),
            i,
            i
        );
    }

    private void bulk(String index, String ndjson) throws IOException {
        Request request = new Request("POST", "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(ndjson);
        assertEquals(false, assertOkAndParse(client().performRequest(request), "bulk " + index).get("errors"));
    }

    @SuppressWarnings("unchecked")
    private int shardOf(String index, String id) throws IOException {
        Request request = new Request("GET", "/" + index + "/_search_shards");
        request.addParameter("routing", id);
        List<List<Map<String, Object>>> shards = (List<List<Map<String, Object>>>) entityAsMap(client().performRequest(request)).get("shards");
        return ((Number) shards.get(0).get(0).get("shard")).intValue();
    }

    /** The {@code parquet} directory of the shard's primary, located through the hosting node's data paths. */
    @SuppressWarnings("unchecked")
    private Path parquetDir(String index, int shard) throws IOException {
        Request shards = new Request("GET", "/_cat/shards/" + index);
        shards.addParameter("format", "json");
        shards.addParameter("h", "shard,prirep,node");
        String node = entityAsList(client().performRequest(shards)).stream()
            .map(row -> (Map<String, Object>) row)
            .filter(row -> String.valueOf(shard).equals(row.get("shard")) && "p".equals(row.get("prirep")))
            .map(row -> (String) row.get("node"))
            .findFirst()
            .orElseThrow();
        Map<String, Object> settings = (Map<String, Object>) entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_settings"))).get(index);
        String uuid = (String) ((Map<String, Object>) ((Map<String, Object>) settings.get("settings")).get("index")).get("uuid");
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(client().performRequest(new Request("GET", "/_nodes/" + node + "/stats/fs"))).get("nodes");
        for (Object stats : nodes.values()) {
            for (Map<String, Object> data : (List<Map<String, Object>>) ((Map<String, Object>) ((Map<String, Object>) stats).get("fs")).get("data")) {
                Path shardDir = Paths.get((String) data.get("path")).resolve("indices").resolve(uuid).resolve(String.valueOf(shard));
                if (Files.isDirectory(shardDir)) {
                    return shardDir.resolve("parquet");
                }
            }
        }
        throw new AssertionError("data directory of " + index + "[" + shard + "] not found on node " + node);
    }

    private static List<Path> parquetFiles(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".parquet")).sorted().collect(Collectors.toList());
        }
    }
}
