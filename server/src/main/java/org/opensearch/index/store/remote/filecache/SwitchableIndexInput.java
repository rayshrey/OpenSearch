/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;


import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SwitchableIndexInput extends IndexInput {

    private final AtomicReference<IndexInput> underlyingIndexInput = new AtomicReference<>();
    private final AtomicReference<IndexInput> localIndexInput = new AtomicReference<>();
    private final AtomicReference<IndexInput> remoteIndexInput = new AtomicReference<>();;
    private final FileCache fileCache;
    private final Path fullFilePath;
    private final Path switchableFilePath;
    private final String fileName;
    private final long fileLength;
    private final long offset;
    private final FSDirectory localDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final TransferManager transferManager;
    private final boolean isClone;
    private volatile boolean isClosed;
    private final AtomicBoolean hasSwitched;
    private final Set<SwitchableIndexInput> clones;

    public SwitchableIndexInput(
        FileCache fileCache,
        String fileName,
        Path fullFilePath,
        Path switchableFilePath,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager) throws IOException {
        this("SwitchableIndexInput (path=" + fullFilePath.toString() + ")\"",fileCache, fileName, fullFilePath, switchableFilePath, localDirectory, remoteDirectory, transferManager, 0, localDirectory.fileLength(fileName), false, false, null, null);
    }

    SwitchableIndexInput(
        String resourceDescription,
        FileCache fileCache,
        String fileName,
        Path fullFilePath,
        Path switchableFilePath,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        long offset,
        long fileLength,
        boolean hasSwitched,
        boolean isClone,
        IndexInput clonedLocalIndexInput,
        IndexInput clonedRemoteIndexInput) throws IOException {
        super(resourceDescription);
        this.fileCache = fileCache;
        this.fullFilePath = fullFilePath;
        this.switchableFilePath = switchableFilePath;
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.fileName = fileName;
        this.offset = offset;
        this.fileLength = fileLength;
        this.transferManager = transferManager;
        this.isClone = isClone;
        this.hasSwitched = new AtomicBoolean(hasSwitched);
        this.isClosed = false;
        clones = new HashSet<>();
        if (!isClone) {
            fileCache.put(fullFilePath, new CachedFullFileIndexInput(fileCache, fullFilePath, localDirectory.openInput(fileName, IOContext.DEFAULT)));
            localIndexInput.set(getLocalIndexInput());
            underlyingIndexInput.set(localIndexInput.get());
            fileCache.decRef(fullFilePath);
        } else {
            if (!hasSwitched) {
                localIndexInput.set(clonedLocalIndexInput);
                underlyingIndexInput.set(localIndexInput.get());
            } else {
                remoteIndexInput.set(clonedRemoteIndexInput);
                underlyingIndexInput.set(remoteIndexInput.get());
            }
        }
    }

    public synchronized void switchToRemote() throws IOException {
        if (isClosed || hasSwitched.get())
            return;
        remoteIndexInput.set(getRemoteIndexInput());
        IndexInput localIndexInput = underlyingIndexInput.get();
        remoteIndexInput.get().seek(localIndexInput.getFilePointer());
        underlyingIndexInput.set(remoteIndexInput.get());
        for (SwitchableIndexInput clone: clones) {
            clone.switchToRemote();
        }
        localIndexInput.close();
        hasSwitched.set(true);
        if (!isClone) fileCache.remove(fullFilePath);
    }


    @Override
    public synchronized void close() throws IOException {
        if (!isClosed) {
            for (SwitchableIndexInput clone: clones) {
                clone.close();
            }
            if (localIndexInput.get() != null)
                localIndexInput.get().close();
            if (remoteIndexInput.get() != null)
                remoteIndexInput.get().close();
            if (isClone) {
                fileCache.decRef(switchableFilePath);
            }
            clones.clear();
            isClosed = true;
        }
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
        fileCache.incRef(switchableFilePath);
        try {
            SwitchableIndexInput clonedIndexInput = new SwitchableIndexInput("SwitchableIndexInput Clone (path=" + fullFilePath.toString() + ")\"", fileCache, fileName, fullFilePath, switchableFilePath, localDirectory, remoteDirectory, transferManager, this.offset, this.fileLength, hasSwitched.get(), true,
                (!hasSwitched.get() && localIndexInput.get() != null) ? localIndexInput.get().clone() : null,
                (hasSwitched.get() && remoteIndexInput.get() != null) ? remoteIndexInput.get().clone() : null);
            clonedIndexInput.seek(getFilePointer());
            clones.add(clonedIndexInput);
            return clonedIndexInput;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        fileCache.incRef(switchableFilePath);
        try {
            SwitchableIndexInput slicedIndexInput =  new SwitchableIndexInput("SwitchableIndexInput Slice " + sliceDescription + "(path=" + fullFilePath.toString() + ")\"", fileCache, fileName, fullFilePath, switchableFilePath, localDirectory, remoteDirectory, transferManager, this.offset + offset, length, hasSwitched.get(), true,
                (!hasSwitched.get() && localIndexInput.get() != null) ? localIndexInput.get().slice(sliceDescription, offset, length) : null,
                (hasSwitched.get() && remoteIndexInput.get() != null) ? remoteIndexInput.get().slice(sliceDescription, offset, length) : null);
            slicedIndexInput.seek(0);
            clones.add(slicedIndexInput);
            return slicedIndexInput;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        IndexInput indexInput =  fileCache.get(fullFilePath).getIndexInput().clone();
        fileCache.decRef(fullFilePath);
        return indexInput;
    }

    private synchronized IndexInput getRemoteIndexInput() throws IOException {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
            .get(fileName);
        BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
            fileName,
            new StoreFileMetadata(fileName, uploadedSegmentMetadata.getLength(), uploadedSegmentMetadata.getChecksum(), Version.LATEST),
            null
        );
        return new OnDemandBlockSnapshotIndexInput(fileInfo, localDirectory, transferManager).slice("Switched Index Input " + fileName, offset, fileLength);
    }
}
