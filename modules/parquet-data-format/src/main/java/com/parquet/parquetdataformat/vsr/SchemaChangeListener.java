/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Listener for schema changes in ManagedVSR.
 */
public interface SchemaChangeListener {
    boolean preSchemaChange();
    boolean postSchemaChange(Schema newSchema);
}
