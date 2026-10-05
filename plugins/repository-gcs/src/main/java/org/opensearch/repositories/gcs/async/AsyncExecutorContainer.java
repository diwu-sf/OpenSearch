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
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.unit.TimeValue;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Virtual executor and bounded operation admission for one GCS client priority group. */
@InternalApi
public final class AsyncExecutorContainer {
    private static final Logger logger = LogManager.getLogger(AsyncExecutorContainer.class);
    private final Executor asyncExecutor;
    private final Semaphore activeOperations;
    private final Semaphore outstandingOperations;
    private final TimeValue acquireTimeout;

    public AsyncExecutorContainer(Executor executor, int maxConcurrency, int maxPending, TimeValue acquireTimeout) {
        this.asyncExecutor = Objects.requireNonNull(executor);
        this.activeOperations = new Semaphore(maxConcurrency, true);
        this.outstandingOperations = new Semaphore(Math.addExact(maxConcurrency, maxPending));
        this.acquireTimeout = acquireTimeout;
    }

    public <T> CompletableFuture<T> executeAsync(CheckedSupplier<T, IOException> operation) {
        return submitAsync(() -> {
            try (RequestPermit permit = acquirePermit()) {
                return operation.get();
            }
        }, ignored -> {}, false);
    }

    public CompletableFuture<InputStreamContainer> openReadStreamAsync(CheckedSupplier<InputStreamContainer, IOException> operation) {
        return submitAsync(() -> {
            RequestPermit permit = acquirePermit();
            boolean handedOff = false;
            try {
                InputStreamContainer part = operation.get();
                InputStream input = new FilterInputStream(part.getInputStream()) {
                    private final AtomicBoolean closed = new AtomicBoolean();

                    @Override
                    public void close() throws IOException {
                        if (closed.compareAndSet(false, true)) {
                            try {
                                super.close();
                            } finally {
                                permit.close();
                                outstandingOperations.release();
                            }
                        }
                    }
                };
                InputStreamContainer result = new InputStreamContainer(input, part.getContentLength(), part.getOffset());
                handedOff = true;
                return result;
            } finally {
                if (handedOff == false) {
                    permit.close();
                }
            }
        }, part -> {
            try {
                part.getInputStream().close();
            } catch (IOException e) {
                logger.warn("Failed to close a cancelled GCS read", e);
            }
        }, true);
    }

    private RequestPermit acquirePermit() throws InterruptedException {
        if (activeOperations.tryAcquire(acquireTimeout.nanos(), TimeUnit.NANOSECONDS) == false) {
            throw new RejectedExecutionException("Timed out acquiring a GCS operation permit");
        }
        return new RequestPermit();
    }

    private class RequestPermit implements AutoCloseable {
        @Override
        public void close() {
            activeOperations.release();
        }
    }

    private <T> CompletableFuture<T> submitAsync(CheckedSupplier<T, Exception> operation, Consumer<T> onDiscard, boolean retainAdmission) {
        final Executor executor = asyncExecutor;
        if (outstandingOperations.tryAcquire() == false) {
            return CompletableFuture.failedFuture(new RejectedExecutionException("Too many outstanding GCS operations"));
        }
        final CompletableFuture<T> result = new CompletableFuture<>();
        try {
            // Keep the supplier future private: cancelling a waiting read must not bypass permit cleanup.
            CompletableFuture.supplyAsync(() -> {
                boolean succeeded = false;
                try {
                    T value = operation.get();
                    succeeded = true;
                    return value;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(e);
                } catch (Exception e) {
                    throw new CompletionException(e);
                } finally {
                    // Open reads retain both admission and active-operation permits until close.
                    if (retainAdmission == false || succeeded == false) {
                        outstandingOperations.release();
                    }
                }
            }, executor).whenComplete((value, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(failure);
                } else if (result.complete(value) == false) {
                    onDiscard.accept(value);
                }
            });
        } catch (RuntimeException e) {
            outstandingOperations.release();
            result.completeExceptionally(e);
        }
        return result;
    }

}
