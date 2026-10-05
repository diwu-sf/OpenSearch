/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.RejectedExecutionException;

public class GoogleCloudStorageAsyncServiceTests extends OpenSearchTestCase {
    public void testS3StylePermitDefaults() {
        Settings settings = Settings.builder().put("node.processors", 1).build();
        ThreadContext context = new ThreadContext(settings);
        try (
            GoogleCloudStorageAsyncService service = new GoogleCloudStorageAsyncService(
                settings,
                context,
                Runnable::run,
                Runnable::run,
                Runnable::run
            )
        ) {
            assertEquals(7, service.transferManager().getTransferSemaphoresHolder().getNormalPriorityPermits());
            assertEquals(3, service.transferManager().getTransferSemaphoresHolder().getLowPriorityPermits());
        }
        GoogleCloudStorageClientSettings defaults = GoogleCloudStorageClientSettings.load(Settings.EMPTY).get("default");
        assertEquals(500, defaults.getMaxConcurrentOperations());
        assertEquals(10_000, defaults.getMaxPendingOperations());
        assertEquals(TimeValue.timeValueMinutes(15), defaults.getOperationAcquisitionTimeout());
    }

    public void testNamedClientLimitsAndReload() {
        Settings settings = Settings.builder()
            .put("gcs.client.custom.max_concurrent_operations", 3)
            .put("gcs.client.custom.max_pending_operations", 7)
            .put("gcs.client.custom.operation_acquisition_timeout", "5s")
            .build();
        var clientSettings = GoogleCloudStorageClientSettings.load(settings);
        var custom = clientSettings.get("custom");
        assertEquals(3, custom.getMaxConcurrentOperations());
        assertEquals(7, custom.getMaxPendingOperations());
        assertEquals(TimeValue.timeValueSeconds(5), custom.getOperationAcquisitionTimeout());
        ThreadContext context = new ThreadContext(settings);
        try (
            GoogleCloudStorageAsyncService service = new GoogleCloudStorageAsyncService(
                settings,
                context,
                Runnable::run,
                Runnable::run,
                Runnable::run
            )
        ) {
            var clients = service.client("custom", custom);
            assertSame(clients, service.client("custom", custom));
            assertNotSame(clients.client(), clients.priorityClient());
            assertNotSame(clients.priorityClient(), clients.urgentClient());
            assertNotSame(clients.client(), service.client("default", clientSettings.get("default")).client());
            assertNotSame(clients, service.client("custom", GoogleCloudStorageClientSettings.load(settings).get("custom")));
            service.close();
            expectThrows(RejectedExecutionException.class, () -> service.client("custom", custom));
        }
    }

    public void testInvalidLimitsRejected() {
        for (String key : new String[] { "max_concurrent_operations", "max_pending_operations" }) {
            Settings invalid = Settings.builder().put("gcs.client.default." + key, -1).build();
            expectThrows(IllegalArgumentException.class, () -> GoogleCloudStorageClientSettings.load(invalid));
        }
    }
}
