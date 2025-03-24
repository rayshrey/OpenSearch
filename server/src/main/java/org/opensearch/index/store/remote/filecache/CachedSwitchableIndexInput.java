/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachedSwitchableIndexInput implements CachedIndexInput {

    private final SwitchableIndexInput switchableIndexInput;
    private final AtomicBoolean isClosed;

    public CachedSwitchableIndexInput(
        FileCache fileCache,
        Path path,
        IndexInput indexInput,
        String name,
        RemoteSegmentStoreDirectory remoteDirectory,
        FSDirectory localDirectory,
        TransferManager transferManager
        ) throws IOException {
        isClosed = new AtomicBoolean(false);
        switchableIndexInput = new SwitchableIndexInput(fileCache, path, indexInput, name,remoteDirectory, localDirectory, transferManager);
    }

    @Override
    public IndexInput getIndexInput() throws IOException {
        if (isClosed.get()) throw new AlreadyClosedException("Index input is already closed");
        return switchableIndexInput;
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() throws Exception {
        if (!isClosed.getAndSet(true)) {
            switchableIndexInput.close();
        }
    }
}
