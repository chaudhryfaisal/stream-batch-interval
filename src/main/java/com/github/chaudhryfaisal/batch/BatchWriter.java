package com.github.chaudhryfaisal.batch;

/**
 * Batch Writer to implement different strategies for batch processing
 */
public interface BatchWriter<T> {
    /**
     * Consume single event
     *
     * @param sink  to drain events to
     * @param batch to write
     */
    void write(Sink<T, ?> sink, T batch);

    /**
     * FLush all cached
     */
    void close();
}