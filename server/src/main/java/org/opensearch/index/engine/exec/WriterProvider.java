/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

/**
 * Generic interface for extracting format-specific writers from composite writers.
 * Allows document inputs to retrieve their corresponding writer without direct casting.
 */
public interface WriterProvider {
    /**
     * Get the writer for a specific data format.
     *
     * @param dataFormatName the name of the data format
     * @return the writer for that format, or null if not found
     */
    Writer<?> getWriter(String dataFormatName);
}
