/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.queue.LockableConcurrentQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompositeDataFormatWriterPool implements Iterable<CompositeDataFormatWriter>, Closeable {

    private final Map<Long, Set<CompositeDataFormatWriter>> writersByVersion;
    private final Map<Long, LockableConcurrentQueue<CompositeDataFormatWriter>> availableWritersByVersion;
    private final Function<Long, CompositeDataFormatWriter> writerSupplier;
    private final Supplier<Queue<CompositeDataFormatWriter>> queueSupplier;
    private final int concurrency;
    private volatile boolean closed;

    public CompositeDataFormatWriterPool(
        Function<Long, CompositeDataFormatWriter> writerSupplier,
        Supplier<Queue<CompositeDataFormatWriter>> queueSupplier,
        int concurrency
    ) {
        this.writersByVersion = new ConcurrentHashMap<>();
        this.availableWritersByVersion = new ConcurrentHashMap<>();
        this.writerSupplier = writerSupplier;
        this.queueSupplier = queueSupplier;
        this.concurrency = concurrency;
    }

    public CompositeDataFormatWriter getAndLock(long mappingVersion) {
        ensureOpen();
        LockableConcurrentQueue<CompositeDataFormatWriter> versionQueue = 
            availableWritersByVersion.computeIfAbsent(mappingVersion, 
                k -> new LockableConcurrentQueue<>(queueSupplier, concurrency));
        
        CompositeDataFormatWriter writer = versionQueue.lockAndPoll();
        if (writer != null) {
            return writer;
        }
        return fetchWriter(mappingVersion);
    }

    private CompositeDataFormatWriter fetchWriter(long mappingVersion) {
        ensureOpen();
        CompositeDataFormatWriter writer = writerSupplier.apply(mappingVersion);
        writer.lock();
        writersByVersion.computeIfAbsent(mappingVersion, 
            k -> Collections.newSetFromMap(new IdentityHashMap<>())).add(writer);
        return writer;
    }

    public void releaseAndUnlock(CompositeDataFormatWriter state) {
        assert
            !state.isFlushPending() && !state.isAborted() :
            "CompositeDataFormatWriter has pending flush: " + state.isFlushPending() + " aborted=" + state.isAborted();
        assert isRegistered(state) : "CompositeDocumentWriterPool doesn't know about this CompositeDataFormatWriter";
        
        LockableConcurrentQueue<CompositeDataFormatWriter> versionQueue = 
            availableWritersByVersion.get(state.getMappingVersion());
        if (versionQueue != null) {
            versionQueue.addAndUnlock(state);
        }
    }

    public List<CompositeDataFormatWriter> checkoutAll() {
        ensureOpen();
        List<CompositeDataFormatWriter> lockedWriters = new ArrayList<>();
        List<CompositeDataFormatWriter> checkedOutWriters = new ArrayList<>();
        
        for (CompositeDataFormatWriter writer : this) {
            writer.lock();
            lockedWriters.add(writer);
        }
        
        synchronized (this) {
            for (CompositeDataFormatWriter writer : lockedWriters) {
                try {
                    if (isRegistered(writer)) {
                        Set<CompositeDataFormatWriter> versionWriters = writersByVersion.get(writer.getMappingVersion());
                        if (versionWriters != null && versionWriters.remove(writer)) {
                            LockableConcurrentQueue<CompositeDataFormatWriter> versionQueue = 
                                availableWritersByVersion.get(writer.getMappingVersion());
                            if (versionQueue != null) {
                                versionQueue.remove(writer);
                            }
                            writer.setFlushPending();
                            checkedOutWriters.add(writer);
                        }
                    }
                } finally {
                    writer.unlock();
                }
            }
        }
        
        return Collections.unmodifiableList(checkedOutWriters);
    }

    public List<CompositeDataFormatWriter> checkoutByVersion(long mappingVersion) {
        ensureOpen();
        List<CompositeDataFormatWriter> checkedOutWriters = new ArrayList<>();
        
        Set<CompositeDataFormatWriter> versionWriters = writersByVersion.get(mappingVersion);
        if (versionWriters != null) {
            synchronized (this) {
                for (CompositeDataFormatWriter writer : versionWriters) {
                    writer.setFlushPending();
                    checkedOutWriters.add(writer);
                }
            }
        }
        
        return Collections.unmodifiableList(checkedOutWriters);
    }

    synchronized boolean isRegistered(CompositeDataFormatWriter writer) {
        return writersByVersion.values().stream()
            .anyMatch(set -> set.contains(writer));
    }

    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("CompositeDocumentWriterPool is already closed");
        }
    }

    @Override
    public synchronized Iterator<CompositeDataFormatWriter> iterator() {
        List<CompositeDataFormatWriter> allWriters = new ArrayList<>();
        for (Set<CompositeDataFormatWriter> versionWriters : writersByVersion.values()) {
            allWriters.addAll(versionWriters);
        }
        return allWriters.iterator();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}
