/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.common.Nullable;
import org.opensearch.index.Message;

/**
 * Kafka message
 */
public class KafkaMessage implements Message<byte[]> {
    private final byte[] key;
    private final byte[] payload;
    private final Long timestamp;

    /**
     * Constructor
     * @param key the key of the message
     * @param payload the payload of the message
     * @param timestamp the timestamp of the message in milliseconds
     */
    public KafkaMessage(@Nullable byte[] key, byte[] payload, Long timestamp) {
        this.key = key;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    /**
     * Get the key of the message
     * @return the key of the message
     */
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }
}
