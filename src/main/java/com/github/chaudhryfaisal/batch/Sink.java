package com.github.chaudhryfaisal.batch;

import java.util.Collection;

/**
 * Sink Interface to receive and process data in batches
 */
public interface Sink<B, E> extends AutoCloseable {
    /**
     * Write a bulk Events to backend
     *
     * @param payload The point to write
     */
    public void write(final B payload);

    /**
     * Transform list of events to bulk object
     *
     * @param events The events to be bulked
     */
    public B listToBulkPayload(final Collection<E> events);

    /**
     * Send any buffered points backend
     */
    public void flush();

    /**
     * close threads
     */
    public void close();

}
