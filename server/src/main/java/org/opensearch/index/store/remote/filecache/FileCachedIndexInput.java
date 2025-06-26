/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.store.remote.utils.TransferManager.StreamReader;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Reference Counted IndexInput. The first FileCachedIndexInput for a file/block is called origin.
 * origin never references to itself, so the RC = 0 when origin is created.
 * Every time there is a clone to the origin, RC + 1.
 * Every time a clone is closed, RC - 1.
 * When there is an eviction in FileCache, it only cleanups those origins with RC = 0.
 *
 * @opensearch.internal
 */
public class FileCachedIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger logger = LogManager.getLogger(FileCachedIndexInput.class);

    protected final FileCache cache;

    /**
     * on disk file path of this index input
     */
    protected Path filePath;

    /**
     * underlying lucene index input which this IndexInput
     * delegate all its read functions to.
     */
    protected IndexInput luceneIndexInput;

    /** indicates if this IndexInput instance is a clone or not */
    protected final boolean isClone;

    protected volatile boolean closed = false;
    private Exception error = null;

    public FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput) {
        this(cache, filePath, underlyingIndexInput, false);
    }

    FileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput, boolean isClone) {
        super("FileCachedIndexInput (path=" + filePath.toString() + ")");
        this.cache = cache;
        this.filePath = filePath;
        this.luceneIndexInput = underlyingIndexInput;
        this.isClone = isClone;
    }

    @Override
    public long getFilePointer() {
        return luceneIndexInput.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        try {
            luceneIndexInput.seek(pos);
        } catch (IOException e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public long length() {
        return luceneIndexInput.length();
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return luceneIndexInput.readByte();
        } catch (Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return luceneIndexInput.readShort();
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return luceneIndexInput.readInt();
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public long readLong() throws IOException {
        try{
            return luceneIndexInput.readLong();
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public final int readVInt() throws IOException {
        try {
            return luceneIndexInput.readVInt();
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public final long readVLong() throws IOException {
        try {
            return luceneIndexInput.readVLong();
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        try {
            luceneIndexInput.readBytes(b, offset, len);
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public byte readByte(long pos) throws IOException {
        try {
            return ((RandomAccessInput) luceneIndexInput).readByte(pos);
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        try {
            return ((RandomAccessInput) luceneIndexInput).readShort(pos);
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        try {
            return ((RandomAccessInput) luceneIndexInput).readInt(pos);
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        try {
            return ((RandomAccessInput) luceneIndexInput).readLong(pos);
        } catch(Exception e) {
            logErrors();
            throw e;
        }
    }

    @Override
    public FileCachedIndexInput clone() {
        cache.incRef(filePath);
        return new FileCachedIndexInput(cache, filePath, luceneIndexInput.clone(), true);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        // never reach here!
        throw new UnsupportedOperationException("FileCachedIndexInput couldn't be sliced.");
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            // if the underlying lucene index input is a clone,
            // the following line won't close/unmap the file.
            luceneIndexInput.close();
            luceneIndexInput = null;
            // origin never reference it itself, only clone needs decRef here
            if (isClone) {
                cache.decRef(filePath);
            } else {
                try {
                    throw new Exception("Throwing exception to print stack trace of closing the lucene index input");
                } catch (Exception e) {
                    error = e;
                }
            }
            closed = true;
        }
    }

    private void logErrors() {
        StringBuilder stringBuilder = new StringBuilder("Operation failed in FileCachedIndexInput \n");
        stringBuilder.append("FileCachedIndexInput : ").append(this).append("\n")
            .append("isClone : ").append(isClone).append("\n")
            .append("isClosed : ").append(closed);
        try {
            stringBuilder.append("length : ").append(length()).append("\n")
                .append("file_pointer : ").append(getFilePointer());
            CachedIndexInput indexInput = cache.get(filePath);
            if (indexInput != null) {
                stringBuilder.append("File cache entry present, isClosed : ").append(indexInput.isClosed());
                cache.decRef(filePath);
            } else {
                stringBuilder.append("File cache entry not present ");
            }
        } catch (Exception e) {
            logger.error("Error while getting file length and file pointer of FileCachedIndexInput", e);
            e.printStackTrace();
        }
        logger.error(stringBuilder);
        try {
            throw new Exception("Throwing exception to log stacktrace");
        } catch (Exception e) {
            logger.error("Stack trace of error : ");
            e.printStackTrace();
            if (error == null) {
                logger.error("IndexInput not closed yet, hence no stacktrace of closing");
            } else {
                logger.error("Stack trace of index input being closed : ");
                error.printStackTrace();
            }
        }
    }
}
