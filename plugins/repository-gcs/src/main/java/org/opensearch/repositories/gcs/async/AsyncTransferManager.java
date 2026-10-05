/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** S3-style transfer admission, with a transfer permit held for a whole GCS resumable session. */
@InternalApi
public final class AsyncTransferManager implements Closeable {
    private final TransferSemaphoresHolder transferSemaphoresHolder;
    private final SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ;
    private final SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ;
    private final ThreadContext threadContext;
    private volatile boolean closed;

    public AsyncTransferManager(
        TransferSemaphoresHolder permits,
        SizeBasedBlockingQ normalQueue,
        SizeBasedBlockingQ lowQueue,
        ThreadContext threadContext
    ) {
        this.transferSemaphoresHolder = permits;
        this.normalPrioritySizeBasedBlockingQ = normalQueue;
        this.lowPrioritySizeBasedBlockingQ = lowQueue;
        this.threadContext = threadContext;
    }

    public CompletableFuture<Void> uploadObject(AsyncExecutorContainer client, WriteContext context, CheckedRunnable<IOException> upload) {
        if (closed) {
            return CompletableFuture.failedFuture(new java.util.concurrent.RejectedExecutionException("GCS transfer manager is closed"));
        }
        WritePriority priority = context.getWritePriority();
        if (priority == WritePriority.HIGH || priority == WritePriority.URGENT) {
            return client.executeAsync(() -> {
                upload.run();
                return null;
            });
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        Runnable startUpload = threadContext.preserveContext(() -> {
            if (result.isCancelled()) {
                return;
            }
            try {
                TransferSemaphoresHolder.Permit permit = transferSemaphoresHolder.acquirePermit(
                    priority,
                    transferSemaphoresHolder.createRequestContext()
                );
                if (result.isCancelled()) {
                    permit.close();
                    return;
                }
                try {
                    client.executeAsync(() -> {
                        upload.run();
                        return null;
                    }).whenComplete((response, failure) -> {
                        permit.close();
                        if (failure == null) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(failure);
                        }
                    });
                } catch (Exception e) {
                    permit.close();
                    result.completeExceptionally(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result.completeExceptionally(e);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        });
        try {
            SizeBasedBlockingQ queue = priority == WritePriority.LOW ? lowPrioritySizeBasedBlockingQ : normalPrioritySizeBasedBlockingQ;
            queue.produce(new SizeBasedBlockingQ.Item(context.getFileSize(), startUpload, result::completeExceptionally));
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    public TransferSemaphoresHolder getTransferSemaphoresHolder() {
        return transferSemaphoresHolder;
    }

    @Override
    public void close() {
        closed = true;
        normalPrioritySizeBasedBlockingQ.close();
        lowPrioritySizeBasedBlockingQ.close();
    }
}
