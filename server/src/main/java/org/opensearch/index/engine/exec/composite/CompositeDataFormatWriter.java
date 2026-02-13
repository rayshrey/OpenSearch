/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.apache.lucene.util.SetOnce;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.RowIdGenerator;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.WriterProvider;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class CompositeDataFormatWriter implements Writer<CompositeDataFormatWriter.CompositeDocumentInput>, Lock, WriterProvider {

    private final List<Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>>> writers;
    private final Runnable postWrite;
    private final ReentrantLock lock;
    private final SetOnce<Boolean> flushPending = new SetOnce<>();
    private final SetOnce<Boolean> hasFlushed = new SetOnce<>();
    private final long writerGeneration;
    private final long mappingVersion;
    private boolean aborted;
    private final RowIdGenerator rowIdGenerator;
    public static final String ROW_ID = "___row_id";

    public CompositeDataFormatWriter(CompositeIndexingExecutionEngine engine, long writerGeneration, long mappingVersion) {
        this.writers = new ArrayList<>();
        this.lock = new ReentrantLock();
        this.aborted = false;
        this.writerGeneration = writerGeneration;
        this.mappingVersion = mappingVersion;
        engine.getDelegates().forEach(delegate -> {
            try {
                writers.add(new AbstractMap.SimpleImmutableEntry<>(delegate.getDataFormat(), delegate.createWriter(writerGeneration)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        this.postWrite = () -> {
            engine.getDataFormatWriterPool().releaseAndUnlock(this);
        };
        this.rowIdGenerator = new RowIdGenerator(CompositeDataFormatWriter.class.getName());
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput d) throws IOException {
        return d.addToWriter(this);
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        FileInfos.Builder builder = FileInfos.builder();
        for (Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>> writerPair : writers) {
            Optional<WriterFileSet> writerFileSetOptional = writerPair.getValue().flush(flushIn).getWriterFileSet(writerPair.getKey());
            writerFileSetOptional.ifPresent(fileMetadata -> builder.putWriterFileSet(writerPair.getKey(), fileMetadata));
        }
        hasFlushed.set(true);
        return builder.build();
    }

    @Override
    public void sync() throws IOException {

    }

    public void performPostWrite() {
        postWrite.run();
    }

    @Override
    public void close() {
        for (Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>> writerPair : writers) {
            writerPair.getValue().close();
        }
    }

    @Override
    public Writer<?> getWriter(String dataFormatName) {
        return writers.stream()
            .filter(entry -> entry.getKey().name().equals(dataFormatName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }

    void abort() throws IOException {
        aborted = true;
    }

    public void setFlushPending() {
        flushPending.set(Boolean.TRUE);
    }

    public boolean isFlushPending() {
        return flushPending.get() == Boolean.TRUE;
    }

    public boolean isAborted() {
        return aborted;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    public long getWriterGeneration() {
        return writerGeneration;
    }

    public long getMappingVersion() {
        return mappingVersion;
    }

    public List<Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>>> getWriters() {
        return writers;
    }

    public RowIdGenerator getRowIdGenerator() {
        return rowIdGenerator;
    }

    public static class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

        List<? extends DocumentInput<?>> inputs;
        Runnable onClose;
        private long version = -1;
        private long seqNo = -2L;
        private long primaryTerm = 0;

        public CompositeDocumentInput(List<? extends DocumentInput<?>> inputs, Runnable onClose) {
            this.inputs = inputs;
            this.onClose = onClose;
        }

        @Override
        public void addRowIdField(String fieldName, long rowId) {
            for (DocumentInput<?> input : inputs) {
                input.addRowIdField(fieldName, rowId);
            }
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            for (DocumentInput<?> input : inputs) {
                input.addField(fieldType, value);
            }
        }

        @Override
        public void setVersion(long version) {
            this.version = version;
            addField(VersionFieldMapper.VersionFieldType.INSTANCE, version);
        }

        @Override
        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
            addField(SeqNoFieldMapper.SeqNoFieldType.INSTANCE, seqNo);
        }

        @Override
        public void setPrimaryTerm(String fieldName, long primaryTerm) {
            this.primaryTerm = primaryTerm;
            for (DocumentInput<?> input : inputs) {
                input.setPrimaryTerm(fieldName, primaryTerm);
            }
        }

        @Override
        public List<? extends DocumentInput<?>> getFinalInput() {
            return null;
        }

        @Override
        public WriteResult addToWriter(WriterProvider writerProvider) throws IOException {
            WriteResult writeResult = null;
            for (DocumentInput<?> input : inputs) {
                writeResult = input.addToWriter(writerProvider);
            }
            return writeResult;
        }

        @Override
        public void close() throws Exception {
            onClose.run();
        }
    }
}
