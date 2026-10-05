/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.gcs.async.AsyncExecutorContainer;
import org.opensearch.repositories.gcs.async.AsyncTransferManager;
import org.opensearch.repositories.gcs.async.SizeBasedBlockingQ;
import org.opensearch.repositories.gcs.async.TransferSemaphoresHolder;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/** Async admission shared by repositories, with independent clients for URGENT, HIGH and NORMAL/LOW. */
final class GoogleCloudStorageAsyncService implements Closeable {
    static final String ASYNC_TRANSFER = "gcs_async_transfer";
    static final String PRIORITY_ASYNC_TRANSFER = "gcs_priority_async_transfer";
    static final String URGENT_ASYNC_TRANSFER = "gcs_urgent_async_transfer";
    static final Setting<Integer> PRIORITY_PERMIT_ALLOCATION_PERCENT = Setting.intSetting(
        "gcs_priority_permit_alloc_perc",
        70,
        21,
        80,
        Setting.Property.NodeScope
    );
    static final Setting<Integer> PERMIT_WAIT_DURATION_MIN = Setting.intSetting(
        "gcs_permit_wait_duration_min",
        5,
        1,
        10,
        Setting.Property.NodeScope
    );
    static final Setting<Integer> TRANSFER_QUEUE_CONSUMERS = new Setting<>(
        "gcs_transfer_queue_consumers",
        settings -> Integer.toString(Math.max(5, OpenSearchExecutors.allocatedProcessors(settings) * 2)),
        value -> Setting.parseInt(value, 5, "gcs_transfer_queue_consumers"),
        Setting.Property.NodeScope
    );
    static final int MAX_QUEUED_TRANSFERS = 10_000;

    private final Executor normalExecutor;
    private final Executor priorityExecutor;
    private final Executor urgentExecutor;
    private final Map<String, CachedClients> clients = new ConcurrentHashMap<>();
    private final AsyncTransferManager transferManager;
    private volatile boolean closed;

    GoogleCloudStorageAsyncService(Settings settings, ThreadPool threadPool) {
        this(
            settings,
            threadPool.getThreadContext(),
            threadPool.executor(ASYNC_TRANSFER),
            threadPool.executor(PRIORITY_ASYNC_TRANSFER),
            threadPool.executor(URGENT_ASYNC_TRANSFER)
        );
    }

    GoogleCloudStorageAsyncService(Settings settings, ThreadContext context, Executor normal, Executor priority, Executor urgent) {
        normalExecutor = normal;
        priorityExecutor = priority;
        urgentExecutor = urgent;
        int availablePermits = Math.max(OpenSearchExecutors.allocatedProcessors(settings) * 4, 10);
        double allocation = PRIORITY_PERMIT_ALLOCATION_PERCENT.get(settings) / 100.0;
        int normalPermits = (int) (allocation * availablePermits);
        TransferSemaphoresHolder permits = new TransferSemaphoresHolder(
            normalPermits,
            availablePermits - normalPermits,
            TimeValue.timeValueMinutes(PERMIT_WAIT_DURATION_MIN.get(settings))
        );
        int normalConsumers = TRANSFER_QUEUE_CONSUMERS.get(settings);
        int lowConsumers = Math.max(2, (int) (((100 - PRIORITY_PERMIT_ALLOCATION_PERCENT.get(settings)) / 100.0) * normalConsumers));
        transferManager = new AsyncTransferManager(
            permits,
            new SizeBasedBlockingQ(ByteSizeUnit.GB.toBytes(normalConsumers * 10L), MAX_QUEUED_TRANSFERS, normalConsumers, normal),
            new SizeBasedBlockingQ(ByteSizeUnit.GB.toBytes(lowConsumers * 20L), MAX_QUEUED_TRANSFERS, lowConsumers, normal),
            context
        );
    }

    synchronized GoogleCloudStorageAsyncClients client(String clientName, GoogleCloudStorageClientSettings settings) {
        if (closed) {
            throw new RejectedExecutionException("GCS async service is closed");
        }
        CachedClients cached = clients.get(clientName);
        if (cached == null || cached.settings != settings) {
            cached = new CachedClients(
                settings,
                new GoogleCloudStorageAsyncClients(
                    executor(normalExecutor, settings),
                    executor(priorityExecutor, settings),
                    executor(urgentExecutor, settings)
                )
            );
            clients.put(clientName, cached);
        }
        return cached.clients;
    }

    private static AsyncExecutorContainer executor(Executor executor, GoogleCloudStorageClientSettings settings) {
        return new AsyncExecutorContainer(
            executor,
            settings.getMaxConcurrentOperations(),
            settings.getMaxPendingOperations(),
            settings.getOperationAcquisitionTimeout()
        );
    }

    AsyncTransferManager transferManager() {
        return transferManager;
    }

    @Override
    public synchronized void close() {
        closed = true;
        transferManager.close();
        clients.clear();
    }

    private record CachedClients(GoogleCloudStorageClientSettings settings, GoogleCloudStorageAsyncClients clients) {
    }
}
