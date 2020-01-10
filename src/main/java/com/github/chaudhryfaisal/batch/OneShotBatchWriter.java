package com.github.chaudhryfaisal.batch;

import lombok.Data;

/**
 * Batch writer that tries to write BatchPoints exactly once.
 */
@Data
public class OneShotBatchWriter implements BatchWriter {

    @Override
    public void write(Sink sink, final Object bulk) {
        sink.write(bulk);
    }

    @Override
    public void close() {

    }
}
