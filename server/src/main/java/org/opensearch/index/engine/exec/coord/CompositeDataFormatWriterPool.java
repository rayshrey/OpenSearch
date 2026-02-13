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
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompositeDataFormatWriterPool implements Iterable<CompositeDataFormatWriter>, Closeable {

    private final Set<CompositeDataFormatWriter> writers;
    private final LockableConcurrentQueue<CompositeDataFormatWriter> availableWriters;
    private final Function<Long, CompositeDataFormatWriter> writerSupplier;
    private volatile boolean closed;

    public CompositeDataFormatWriterPool(
        Function<Long, CompositeDataFormatWriter> writerSupplier,
        Supplier<Queue<CompositeDataFormatWriter>> queueSupplier,
        int concurrency
    ) {
        this.writers = Collections.newSetFromMap(new IdentityHashMap<>());
        this.writerSupplier = writerSupplier;
        this.availableWriters = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    public CompositeDataFormatWriter getAndLock(long mappingVersion) {
        ensureOpen();
        CompositeDataFormatWriter compositeDataFormatWriter = availableWriters.lockAndPoll();
        return Objects.requireNonNullElseGet(compositeDataFormatWriter, () -> fetchWriter(mappingVersion));
    }

    private CompositeDataFormatWriter fetchWriter(long mappingVersion) {
        ensureOpen();
        CompositeDataFormatWriter compositeDataFormatWriter = writerSupplier.apply(mappingVersion);
        compositeDataFormatWriter.lock();
        writers.add(compositeDataFormatWriter);
        return compositeDataFormatWriter;
    }

    /**
     * Release the given {@link CompositeDataFormatWriter} to this pool for reuse if it is currently managed by this
     * pool.
     *
     * @param state {@link CompositeDataFormatWriter} to release to the pool.
     */
    public void releaseAndUnlock(CompositeDataFormatWriter state) {
        assert
            !state.isFlushPending() && !state.isAborted() :
            "CompositeDataFormatWriter has pending flush: " + state.isFlushPending() + " aborted=" + state.isAborted();
        assert isRegistered(state) : "CompositeDocumentWriterPool doesn't know about this CompositeDataFormatWriter";
        availableWriters.addAndUnlock(state);
    }

    /**
     * Lock and checkout all CompositeDataFormatWriters from the pool for flush.
     *
     * @return Unmodifiable list of all CompositeDataFormatWriters locked by current thread.
     */
    public List<CompositeDataFormatWriter> checkoutAll() {
        ensureOpen();
        List<CompositeDataFormatWriter> lockedWriters = new ArrayList<>();
        List<CompositeDataFormatWriter> checkedOutWriters = new ArrayList<>();
        for (CompositeDataFormatWriter compositeDataFormatWriter : this) {
            compositeDataFormatWriter.lock();
            lockedWriters.add(compositeDataFormatWriter);
        }
        synchronized (this) {
            for (CompositeDataFormatWriter compositeDataFormatWriter : lockedWriters) {
                try {
                    // Release this writer if it’s no longer managed by this pool; otherwise, check it out.
                    if (isRegistered(compositeDataFormatWriter) && writers.remove(compositeDataFormatWriter)) {
                        availableWriters.remove(compositeDataFormatWriter);
                        compositeDataFormatWriter.setFlushPending();
                        checkedOutWriters.add(compositeDataFormatWriter);
                    }
                } finally {
                    compositeDataFormatWriter.unlock();
                }
            }
        }
        return Collections.unmodifiableList(checkedOutWriters);
    }

    synchronized boolean isRegistered(CompositeDataFormatWriter perThread) {
        return writers.contains(perThread);
    }

    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("CompositeDocumentWriterPool is already closed");
        }
    }

    @Override
    public synchronized Iterator<CompositeDataFormatWriter> iterator() {
        return List.copyOf(writers).iterator();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}
