package com.parquet.parquetdataformat.writer;

import org.opensearch.index.mapper.MappedFieldType;

/**
 * Represents a field-value pair collected during document input processing.
 * Decouples document input from any specific storage mechanism (VSR, ManagedVSR, etc).
 */
public class FieldValuePair {
    private final MappedFieldType fieldType;
    private final Object value;

    public FieldValuePair(MappedFieldType fieldType, Object value) {
        this.fieldType = fieldType;
        this.value = value;
    }

    public MappedFieldType getFieldType() {
        return fieldType;
    }

    public Object getValue() {
        return value;
    }
}
