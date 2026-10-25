/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SizeBasedBlockingQTests extends OpenSearchTestCase {
    public void testByteLimitAndFifoOrder() {
        List<Runnable> workers = new ArrayList<>();
        List<Integer> order = new ArrayList<>();
        try (SizeBasedBlockingQ queue = new SizeBasedBlockingQ(10, 10, 1, workers::add)) {
            queue.produce(new SizeBasedBlockingQ.Item(6, () -> order.add(1), e -> fail(e.toString())));
            queue.produce(new SizeBasedBlockingQ.Item(4, () -> order.add(2), e -> fail(e.toString())));
            expectThrows(RejectedExecutionException.class, () -> queue.produce(new SizeBasedBlockingQ.Item(1, () -> {}, e -> {})));
            assertEquals(10, queue.getCurrentSize());
            assertEquals(1, workers.size());
            workers.remove(0).run();
            assertEquals(List.of(1, 2), order);
            assertEquals(0, queue.getSize());
            assertEquals(0, queue.getCurrentSize());
        }
    }

    public void testOversizedFileRemainsAsynchronous() {
        List<Runnable> workers = new ArrayList<>();
        AtomicInteger completed = new AtomicInteger();
        try (SizeBasedBlockingQ queue = new SizeBasedBlockingQ(10, 10, 1, workers::add)) {
            queue.produce(new SizeBasedBlockingQ.Item(100, completed::incrementAndGet, e -> fail(e.toString())));
            assertEquals(0, completed.get());
            assertEquals(10, queue.getCurrentSize());
            workers.remove(0).run();
            assertEquals(1, completed.get());
        }
    }

    public void testItemLimitAndShutdownNotifyQueuedTasks() {
        List<Runnable> workers = new ArrayList<>();
        AtomicInteger rejected = new AtomicInteger();
        SizeBasedBlockingQ queue = new SizeBasedBlockingQ(100, 2, 1, workers::add);
        queue.produce(new SizeBasedBlockingQ.Item(0, () -> fail("closed"), e -> rejected.incrementAndGet()));
        queue.produce(new SizeBasedBlockingQ.Item(1, () -> fail("closed"), e -> rejected.incrementAndGet()));
        expectThrows(RejectedExecutionException.class, () -> queue.produce(new SizeBasedBlockingQ.Item(0, () -> {}, e -> {})));
        queue.close();
        workers.remove(0).run();
        assertEquals(2, rejected.get());
        assertEquals(0, queue.getCurrentSize());
        expectThrows(RejectedExecutionException.class, () -> queue.produce(new SizeBasedBlockingQ.Item(1, () -> {}, e -> {})));
    }

    public void testExecutorRejectionNotifiesTaskAndReleasesCapacity() {
        RejectedExecutionException failure = new RejectedExecutionException("closed executor");
        AtomicReference<Exception> notified = new AtomicReference<>();
        try (SizeBasedBlockingQ queue = new SizeBasedBlockingQ(10, 1, 1, command -> { throw failure; })) {
            queue.produce(new SizeBasedBlockingQ.Item(10, () -> fail("rejected"), notified::set));
            assertSame(failure, notified.get());
            assertEquals(0, queue.getSize());
            queue.produce(new SizeBasedBlockingQ.Item(10, () -> fail("rejected"), notified::set));
            assertEquals(0, queue.getCurrentSize());
        }
    }
}
