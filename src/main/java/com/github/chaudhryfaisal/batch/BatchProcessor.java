package com.github.chaudhryfaisal.batch;


import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * A BatchProcessor can be used to add process queue when configured size or interval is reached
 */
@Builder
@Data
public final class BatchProcessor<B, E> {
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final int DEFAULT_ACTION_SIZE = 100;
    public static final int DEFAULT_FLUSH_INTERVAL = 5000;
    public static final int DEFAULT_JITTER_INTERVAL = 0;

    protected final BlockingQueue<E> queue = new LinkedBlockingQueue<>();
    protected final AtomicBoolean INIT = new AtomicBoolean(false);

    @Builder.Default
    private int actions = DEFAULT_ACTION_SIZE;
    @Builder.Default
    private int flushInterval = DEFAULT_FLUSH_INTERVAL;
    @Builder.Default
    private TimeUnit flushIntervalUnit = TimeUnit.MILLISECONDS;
    @Builder.Default
    private int bufferLimit = DEFAULT_BATCH_SIZE;
    @Builder.Default
    private int jitterInterval = DEFAULT_JITTER_INTERVAL;
    private Sink<B, E> sink;
    @Builder.Default
    private BatchWriter<B> writer = new OneShotBatchWriter();
    @Builder.Default
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory());
    private BiConsumer<Iterable<E>, Throwable> exceptionHandler;


    private synchronized void init() {
        Runnable flushRunnable = new Runnable() {
            @Override
            public void run() {
                // write doesn't throw any exceptions
                drain();
                int jitterInterval = (int) (Math.random() * BatchProcessor.this.jitterInterval);
                BatchProcessor.this.scheduler.schedule(this, BatchProcessor.this.flushInterval + jitterInterval, BatchProcessor.this.flushIntervalUnit);
            }
        };
        // Flush at specified Rate
        this.scheduler.schedule(flushRunnable, this.flushInterval + (int) (Math.random() * BatchProcessor.this.jitterInterval), this.flushIntervalUnit);
    }

    /**
     * function to trigger to drain queue (scheduled / action size)
     * Note: used System.err to avoid logger dependency to be used as log shipped
     */
    void drain() {
        List<E> events = new ArrayList<>();
        try {
            if (this.queue.isEmpty()) {
                return;
            }
            events = new ArrayList<>(this.queue.size());
            this.queue.drainTo(events);
            partition(events, bufferLimit).forEach(p -> writer.write(sink, sink.listToBulkPayload(p)));
        } catch (Throwable t) {
            exceptionHandler.accept(events, t);
            System.err.println("BatchProcessor -> Batch could not be sent. Data will be lost \n" + t.getLocalizedMessage());
        }
    }

    /**
     * @param event the event to be queued.
     */
    public void add(final E event) {
        if (!INIT.getAndSet(true)) {
            init();
        }
        try {
            this.queue.put(event);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (this.queue.size() >= this.actions) {
            this.scheduler.submit(this::drain);
        }
    }


    /**
     * Force and shut down and sub threads
     */
    public void close() {
        this.drain();
        if (this.scheduler != null) {
            this.scheduler.shutdown();
        }
        this.writer.close();
    }

    /**
     * Force drain items in queue
     */
    public void flush() {
        this.drain();
    }


    /**
     * Method to split list into batch size to be processed in batches
     */
    static <T> List<List<T>> partition(List<T> list, final int size) {
        List<List<T>> parts = new ArrayList<List<T>>();

        final int N = list.size();
        for (int i = 0; i < N; i += size) {
            parts.add(new ArrayList<T>(
                    list.subList(i, Math.min(N, i + size)))
            );
        }
        return parts;
    }
}