/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.InternalApi;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/** NORMAL or LOW transfer queue bounded by queued bytes and item count. Consumers run on virtual threads. */
@InternalApi
public final class SizeBasedBlockingQ implements Closeable {
    private static final Logger logger = LogManager.getLogger(SizeBasedBlockingQ.class);
    private final ArrayDeque<Item> queue = new ArrayDeque<>();
    private final long capacity;
    private final int maxItems;
    private final int consumers;
    private final Executor executor;
    private long currentSize;
    private int runningConsumers;
    private boolean closed;

    public SizeBasedBlockingQ(long capacity, int maxItems, int consumers, Executor executor) {
        if (capacity < 1 || maxItems < 1 || consumers < 1) {
            throw new IllegalArgumentException("Queue capacity and consumer count must be positive");
        }
        this.capacity = capacity;
        this.maxItems = maxItems;
        this.consumers = consumers;
        this.executor = executor;
    }

    public void produce(Item item) {
        boolean startConsumer;
        synchronized (this) {
            if (closed) {
                throw new RejectedExecutionException("GCS transfer queue is closed");
            }
            // An oversized file occupies a whole queue budget, but still uses async resumable upload.
            long charge = charge(item);
            if (queue.size() >= maxItems || charge > capacity - currentSize) {
                throw new RejectedExecutionException("GCS transfer queue capacity reached");
            }
            queue.addLast(item);
            currentSize += charge;
            startConsumer = runningConsumers < consumers;
            if (startConsumer) {
                runningConsumers++;
            }
        }
        if (startConsumer) {
            try {
                executor.execute(this::consume);
            } catch (RuntimeException e) {
                List<Item> rejected;
                synchronized (this) {
                    runningConsumers--;
                    rejected = runningConsumers == 0 ? drain() : List.of();
                }
                rejected.forEach(pending -> reject(pending, e));
            }
        }
    }

    private void consume() {
        while (true) {
            Item item;
            synchronized (this) {
                item = queue.pollFirst();
                if (item == null) {
                    runningConsumers--;
                    return;
                }
                currentSize -= charge(item);
            }
            try {
                item.consumable.run();
            } catch (Exception e) {
                reject(item, e);
            }
        }
    }

    private long charge(Item item) {
        return Math.min(Math.max(1L, item.size), capacity);
    }

    private List<Item> drain() {
        List<Item> pending = new ArrayList<>(queue);
        queue.clear();
        currentSize = 0;
        return pending;
    }

    private static void reject(Item item, Exception failure) {
        try {
            item.onFailure.accept(failure);
        } catch (Exception e) {
            logger.warn("Failed to notify rejected GCS transfer", e);
        }
    }

    public synchronized long getCurrentSize() {
        return currentSize;
    }

    public synchronized int getSize() {
        return queue.size();
    }

    @Override
    public void close() {
        List<Item> pending;
        synchronized (this) {
            closed = true;
            pending = drain();
        }
        RejectedExecutionException failure = new RejectedExecutionException("GCS transfer queue is closed");
        pending.forEach(item -> reject(item, failure));
    }

    public record Item(long size, Runnable consumable, Consumer<Exception> onFailure) {
    }
}
