/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;


import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SwitchableIndexInput extends IndexInput {

    private final AtomicReference<IndexInput> underlyingIndexInput = new AtomicReference<>();
    private final FileCache fileCache;
    private final Path filePath;
    private final String name;
    private final FSDirectory localDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final TransferManager transferManager;
    private final AtomicBoolean hasSwitched = new AtomicBoolean(false);

    public SwitchableIndexInput(FileCache fileCache, Path filePath, IndexInput localIndexInput,
                                String name,
                                RemoteSegmentStoreDirectory remoteDirectory,
                                FSDirectory localDirectory,
                                TransferManager transferManager) throws IOException {
        super("SwitchableIndexInput (path=" + filePath.toString() + ")\"");
        this.fileCache = fileCache;
        this.filePath = filePath;
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.name = name;
        this.transferManager = transferManager;
        fileCache.put(filePath, new CachedFullFileIndexInput(fileCache, filePath, localIndexInput));
        underlyingIndexInput.set(getLocalIndexInput());
    }

    public synchronized void switchToRemote() throws IOException {
        IndexInput remoteIndexInput = getRemoteIndexInput();
        remoteIndexInput.seek(underlyingIndexInput.get().getFilePointer());
        IndexInput localIndexInput = underlyingIndexInput.get();
        underlyingIndexInput.set(remoteIndexInput);
        hasSwitched.set(true);
        localIndexInput.close();
    }


    @Override
    public synchronized void close() throws IOException {
        underlyingIndexInput.get().close();
    }

    @Override
    public synchronized long getFilePointer() {
        return underlyingIndexInput.get().getFilePointer();
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        underlyingIndexInput.get().seek(pos);
    }

    @Override
    public synchronized long length() {
        return underlyingIndexInput.get().length();
    }

    @Override
    public synchronized IndexInput clone() {
        return underlyingIndexInput.get().clone();
    }

    @Override
    public synchronized IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return underlyingIndexInput.get().slice(sliceDescription, offset, length);
    }

    @Override
    public synchronized byte readByte() throws IOException {
        return underlyingIndexInput.get().readByte();
    }

    @Override
    public synchronized void readBytes(byte[] b, int offset, int len) throws IOException {
        underlyingIndexInput.get().readBytes(b,offset, len);
    }

    private synchronized IndexInput getLocalIndexInput() throws IOException {
        IndexInput indexInput =  fileCache.get(filePath).getIndexInput().clone();
        fileCache.decRef(filePath);
        return indexInput;
    }

    private synchronized IndexInput getRemoteIndexInput() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
            .get(name);
        BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
            name,
            new StoreFileMetadata(name, uploadedSegmentMetadata.getLength(), uploadedSegmentMetadata.getChecksum(), Version.LATEST),
            null
        );
        return new OnDemandBlockSnapshotIndexInput(fileInfo, localDirectory, transferManager);
    }
}
