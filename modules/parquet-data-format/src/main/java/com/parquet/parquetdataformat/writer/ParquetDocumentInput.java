package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.fields.ArrowFieldRegistry;
import com.parquet.parquetdataformat.fields.ParquetField;
import org.apache.arrow.vector.BigIntVector;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.WriterProvider;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.MappedFieldType;
import com.parquet.parquetdataformat.vsr.ManagedVSR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Document input wrapper for Parquet-based document processing.
 *
 * <p>Collects fields incrementally in a list of FieldValuePair objects.
 * Completely decoupled from ManagedVSR - fields are transferred only when
 * a compatible writer is obtained and passed to addToWriter().
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    private long rowId = -1;

    public ParquetDocumentInput() {
    }

    @Override
    public void addRowIdField(String fieldName, long rowId) {
        this.rowId = rowId;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        collectedFields.add(new FieldValuePair(fieldType, value));
    }

    @Override
    public void setPrimaryTerm(String fieldName, long primaryTerm) {
        // Stored as a field via addField() by CompositeDocumentInput
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        return collectedFields;
    }

    @Override
    public WriteResult addToWriter(WriterProvider writerProvider) throws IOException {
        ParquetWriter parquetWriter = (ParquetWriter) writerProvider.getWriter("parquet");
        ManagedVSR managedVSR = parquetWriter.getVSRManager().getActiveManagedVSR();

        transferFieldsToVSR(managedVSR);
        return new WriteResult(true, null, 1, 1, 1);
    }

    @Override
    public void close() throws Exception {
        // No cleanup needed
    }

    /**
     * Internal method for VSRManager to transfer collected fields to a ManagedVSR.
     */
    public void transferFieldsToVSR(ManagedVSR managedVSR) throws IOException {
        if (rowId >= 0) {
            BigIntVector bigIntVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
            int rowCount = managedVSR.getRowCount();
            bigIntVector.setSafe(rowCount, rowId);
        }

        for (FieldValuePair pair : collectedFields) {
            MappedFieldType fieldType = pair.getFieldType();
            Object value = pair.getValue();
            final String fieldTypeName = fieldType.typeName();
            final ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldTypeName);

            if (parquetField == null) {
                throw new IllegalArgumentException(
                    String.format("Unsupported field type: %s. Field type is not registered in ArrowFieldRegistry.", fieldTypeName)
                );
            }

            parquetField.createField(fieldType, managedVSR, value);
        }

        int currentRowCount = managedVSR.getRowCount();
        managedVSR.setRowCount(currentRowCount + 1);
    }
}
