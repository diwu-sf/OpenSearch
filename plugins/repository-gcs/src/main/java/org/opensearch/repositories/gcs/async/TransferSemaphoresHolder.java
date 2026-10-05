/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.TimeValue;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Node-wide upload permits, with the same NORMAL/LOW allocation and borrowing policy as S3. */
@InternalApi
public final class TransferSemaphoresHolder {
    private final Semaphore normalPrioritySemaphore;
    private final Semaphore lowPrioritySemaphore;
    private final int normalPriorityPermits;
    private final int lowPriorityPermits;
    private final TimeValue acquireWaitDuration;

    public TransferSemaphoresHolder(int normalPriorityPermits, int lowPriorityPermits, TimeValue acquireWaitDuration) {
        this.normalPriorityPermits = normalPriorityPermits;
        this.lowPriorityPermits = lowPriorityPermits;
        this.normalPrioritySemaphore = new Semaphore(normalPriorityPermits);
        this.lowPrioritySemaphore = new Semaphore(lowPriorityPermits);
        this.acquireWaitDuration = acquireWaitDuration;
    }

    public RequestContext createRequestContext() {
        return new RequestContext(lowPrioritySemaphore.availablePermits() == lowPriorityPermits);
    }

    public Permit acquirePermit(WritePriority priority, RequestContext context) throws InterruptedException {
        if (Objects.requireNonNull(priority) == WritePriority.HIGH || priority == WritePriority.URGENT) {
            return new Permit(null);
        }
        Semaphore acquired = null;
        if (priority == WritePriority.LOW) {
            if (lowPrioritySemaphore.tryAcquire()) {
                acquired = lowPrioritySemaphore;
            } else if (normalPrioritySemaphore.availablePermits() > 0.4 * normalPriorityPermits && normalPrioritySemaphore.tryAcquire()) {
                acquired = normalPrioritySemaphore;
            } else if (lowPrioritySemaphore.tryAcquire(acquireWaitDuration.nanos(), TimeUnit.NANOSECONDS)) {
                acquired = lowPrioritySemaphore;
            }
        } else {
            if (normalPrioritySemaphore.tryAcquire()) {
                acquired = normalPrioritySemaphore;
            } else if (context.lowPriorityPermitsConsumable && lowPrioritySemaphore.tryAcquire()) {
                acquired = lowPrioritySemaphore;
            } else if (normalPrioritySemaphore.tryAcquire(acquireWaitDuration.nanos(), TimeUnit.NANOSECONDS)) {
                acquired = normalPrioritySemaphore;
            }
        }
        if (acquired == null) {
            throw new RejectedExecutionException("Timed out acquiring a GCS transfer permit for " + priority);
        }
        return new Permit(acquired);
    }

    public int getNormalPriorityPermits() {
        return normalPriorityPermits;
    }

    public int getLowPriorityPermits() {
        return lowPriorityPermits;
    }

    public static final class RequestContext {
        private final boolean lowPriorityPermitsConsumable;

        private RequestContext(boolean lowPriorityPermitsConsumable) {
            this.lowPriorityPermitsConsumable = lowPriorityPermitsConsumable;
        }
    }

    /** A transfer owns its permit until completion, including all resumable-session retries. */
    public static final class Permit implements AutoCloseable {
        private final Semaphore semaphore;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Permit(Semaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) && semaphore != null) {
                semaphore.release();
            }
        }
    }
}
