/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for native Parquet writer with lifecycle management and file versioning.
 */
public class NativeParquetWriter implements Closeable {

    private final AtomicBoolean writerClosed = new AtomicBoolean(false);
    private volatile boolean writerInitialized = false;
    private int subVersion = 0;
    private List<ParquetFileMetadata> closedFilesMetadata = new ArrayList<>();
    private String baseFilePath;
    private String currentFilePath;
    private ParquetFileMetadata currentMetadata;

    /**
     * Creates a new native Parquet writer.
     * Writer is initialized lazily on first write.
     * @param filePath path to the Parquet file
     */
    public NativeParquetWriter(String filePath) {
        this.baseFilePath = filePath;
        this.currentFilePath = generateVersionedFileName();
    }

    /**
     * Writes a batch to the Parquet file.
     * Initializes writer on first call.
     * @param arrayAddress Arrow C Data Interface array pointer
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @throws IOException if write fails
     */
    public void write(long arrayAddress, long schemaAddress, long schemaAddressCreate) throws IOException {
        if (!writerInitialized) {
            RustBridge.createWriter(currentFilePath, schemaAddressCreate);
            writerInitialized = true;
        }
        RustBridge.write(currentFilePath, arrayAddress, schemaAddress);
    }

    /**
     * Writes a batch to the Parquet file when writer is already initialized.
     * @param arrayAddress Arrow C Data Interface array pointer
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @throws IOException if write fails
     */
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        RustBridge.write(currentFilePath, arrayAddress, schemaAddress);
    }

    /**
     * Flushes buffered data to disk.
     * @throws IOException if flush fails
     */
    public void flush() throws IOException {
        RustBridge.flushToDisk(currentFilePath);
    }

    /**
     * Rotates to a new Parquet file with updated schema.
     * Closes the current writer and creates a new one.
     *
     * @throws IOException if rotation fails
     */
    public void rotateActiveParquetFile() throws IOException {
        // Close current writer only if it was initialized
        if (writerInitialized && writerClosed.compareAndSet(false, true)) {
            currentMetadata = RustBridge.closeWriter(currentFilePath);
            if (currentMetadata != null) {
                closedFilesMetadata.add(currentMetadata);
            }
        }
        
        // Generate new versioned filename
        currentFilePath = generateVersionedFileName();
        
        // Reset for new writer
        writerClosed.set(false);
        writerInitialized = false;
    }

    /**
     * Generates a versioned filename for schema changes.
     *
     * @return Versioned filename
     */
    private String generateVersionedFileName() {
        int lastDot = baseFilePath.lastIndexOf('.');
        if (lastDot > 0) {
            return baseFilePath.substring(0, lastDot) + ".v" + (subVersion++) + baseFilePath.substring(lastDot);
        }
        return baseFilePath + ".v" + (subVersion++);
    }

    /**
     * Gets all metadata for closed files.
     *
     * @return List of metadata for all closed files
     */
    public List<ParquetFileMetadata> getAllClosedFilesMetadata() {
        return new ArrayList<>(closedFilesMetadata);
    }

    /**
     * Checks if writer has been initialized.
     *
     * @return true if writer is initialized, false otherwise
     */
    public boolean isWriterInitialized() {
        return writerInitialized;
    }

    @Override
    public void close() {
        if (writerInitialized && writerClosed.compareAndSet(false, true)) {
            try {
                currentMetadata = RustBridge.closeWriter(currentFilePath);
                if (currentMetadata != null) {
                    closedFilesMetadata.add(currentMetadata);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to close Parquet writer for " + currentFilePath, e);
            }
        }
    }

    public ParquetFileMetadata getMetadata() {
        return currentMetadata;
    }

    public String getFilePath() {
        return currentFilePath;
    }
}
