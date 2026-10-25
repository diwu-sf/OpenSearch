/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class AsyncTransferManagerTests extends OpenSearchTestCase {
    public void testTransferPermitCoversQueuedClientOperationAndFailure() throws Exception {
        List<Runnable> operations = new ArrayList<>();
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(1, 0, TimeValue.ZERO);
        AsyncExecutorContainer client = new AsyncExecutorContainer(operations::add, 1, 1, TimeValue.ZERO);
        try (AsyncTransferManager manager = manager(holder)) {
            IOException failure = new IOException("upload failed");
            var result = manager.uploadObject(client, context(WritePriority.NORMAL), () -> { throw failure; });
            assertFalse(result.isDone());
            expectThrows(RejectedExecutionException.class, () -> holder.acquirePermit(WritePriority.NORMAL, holder.createRequestContext()));
            operations.remove(0).run();
            assertSame(failure, expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS)).getCause());
            holder.acquirePermit(WritePriority.NORMAL, holder.createRequestContext()).close();
        }
    }

    public void testClientRejectionReleasesTransferPermit() throws Exception {
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(1, 0, TimeValue.ZERO);
        RejectedExecutionException failure = new RejectedExecutionException("client full");
        AsyncExecutorContainer client = new AsyncExecutorContainer(command -> { throw failure; }, 1, 1, TimeValue.ZERO);
        try (AsyncTransferManager manager = manager(holder)) {
            for (int i = 0; i < 3; i++) {
                var result = manager.uploadObject(client, context(WritePriority.NORMAL), () -> fail("client rejected"));
                assertSame(failure, expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS)).getCause());
            }
            holder.acquirePermit(WritePriority.NORMAL, holder.createRequestContext()).close();
        }
    }

    public void testUrgentAndHighBypassTransferPermitsButRejectAfterShutdown() throws Exception {
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(0, 0, TimeValue.ZERO);
        AsyncExecutorContainer client = new AsyncExecutorContainer(Runnable::run, 1, 1, TimeValue.ZERO);
        try (AsyncTransferManager manager = manager(holder)) {
            for (WritePriority priority : List.of(WritePriority.HIGH, WritePriority.URGENT)) {
                manager.uploadObject(client, context(priority), () -> {}).get(30, TimeUnit.SECONDS);
            }
            manager.close();
            for (WritePriority priority : WritePriority.values()) {
                assertTrue(
                    expectThrows(
                        ExecutionException.class,
                        () -> manager.uploadObject(client, context(priority), () -> fail("closed")).get(30, TimeUnit.SECONDS)
                    ).getCause() instanceof RejectedExecutionException
                );
            }
        }
    }

    public void testCancellationBeforeQueueConsumptionDoesNotOpenUpload() {
        List<Runnable> consumers = new ArrayList<>();
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(1, 0, TimeValue.ZERO);
        try (
            AsyncTransferManager manager = new AsyncTransferManager(
                holder,
                new SizeBasedBlockingQ(100, 10, 1, consumers::add),
                new SizeBasedBlockingQ(100, 10, 1, consumers::add),
                new ThreadContext(Settings.EMPTY)
            )
        ) {
            AsyncExecutorContainer client = new AsyncExecutorContainer(Runnable::run, 1, 1, TimeValue.ZERO);
            var result = manager.uploadObject(client, context(WritePriority.NORMAL), () -> fail("cancelled"));
            assertTrue(result.cancel(false));
            consumers.remove(0).run();
            assertTrue(result.isCancelled());
        }
    }

    private AsyncTransferManager manager(TransferSemaphoresHolder holder) {
        return new AsyncTransferManager(
            holder,
            new SizeBasedBlockingQ(100, 10, 1, Runnable::run),
            new SizeBasedBlockingQ(100, 10, 1, Runnable::run),
            new ThreadContext(Settings.EMPTY)
        );
    }

    private WriteContext context(WritePriority priority) {
        return new WriteContext.Builder().fileName("blob").fileSize(3).writePriority(priority).build();
    }
}
