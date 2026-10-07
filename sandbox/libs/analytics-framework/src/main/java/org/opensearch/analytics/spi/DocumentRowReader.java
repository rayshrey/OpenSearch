/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Backend-specific execution contract for reading document rows. Backends implement this
 * to perform the actual storage read (e.g., DataFusion native parquet scan) from a file set
 * that the Core layer has already resolved from the catalog snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentRowReader {

    /**
     * The storage format this backend reads (e.g. {@code "parquet"}). Lets the Core layer
     * resolve the backend's candidate {@link WriterFileSet} from the catalog snapshot.
     */
    String formatName();

    /**
     * Fetch a single row at the given offset from the pre-resolved file set.
     *
     * @param rowId the row offset to fetch
     * @param fileSet the file set to read from
     * @return the row as a field-name → value map, or null if not found
     */
    Map<String, Object> executeSingleRow(long rowId, WriterFileSet fileSet) throws IOException;

    /**
     * Fetch all rows with {@code _seq_no > fromSeqNoExclusive} from the Core-resolved file sets
     * (one per segment for this backend's format).
     *
     * @param fileSets the file sets to scan
     * @param fromSeqNoExclusive the exclusive lower bound on {@code _seq_no}
     */
    List<Map<String, Object>> executeRowsAboveSeqNo(List<WriterFileSet> fileSets, long fromSeqNoExclusive) throws IOException;

    /**
     * Reads several rows of one file in a single pass, keyed by row id.
     *
     * <p>Serves the bulk update prefetch, where many row ids of the same file are known at once.
     * A backend that can satisfy them together amortizes the per-read fixed cost — reader open,
     * boundary crossing, stream setup, and any storage page shared by several rows.
     *
     * <p>A requested id missing from the returned map is not an error: the caller falls back to
     * {@link #executeSingleRow} for it. The default implementation simply loops, so a backend need
     * not override this to stay correct — only to be faster.
     *
     * @param rowIds   row ids to read; must be non-negative
     * @param fileSet  the file the ids belong to
     * @return rows keyed by row id, possibly missing entries
     */
    default Map<Long, Map<String, Object>> executeRows(long[] rowIds, WriterFileSet fileSet) throws IOException {
        Map<Long, Map<String, Object>> rows = new java.util.HashMap<>(rowIds.length);
        for (long rowId : rowIds) {
            Map<String, Object> row = executeSingleRow(rowId, fileSet);
            if (row != null) {
                rows.put(rowId, row);
            }
        }
        return rows;
    }
}
