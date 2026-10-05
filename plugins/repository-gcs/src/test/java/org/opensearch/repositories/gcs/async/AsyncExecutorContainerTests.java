/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class AsyncExecutorContainerTests extends OpenSearchTestCase {
    public void testAcquisitionTimeoutReleasesAdmission() throws Exception {
        AsyncExecutorContainer client = new AsyncExecutorContainer(Runnable::run, 1, 1, TimeValue.ZERO);
        var read = client.openReadStreamAsync(() -> new InputStreamContainer(new ByteArrayInputStream(new byte[0]), 0, 0))
            .get(30, TimeUnit.SECONDS);
        for (int i = 0; i < 3; i++) {
            CompletableFuture<Void> failed = client.executeAsync(() -> null);
            assertTrue(
                expectThrows(ExecutionException.class, () -> failed.get(30, TimeUnit.SECONDS))
                    .getCause() instanceof RejectedExecutionException
            );
        }
        read.getInputStream().close();
        client.executeAsync(() -> null).get(30, TimeUnit.SECONDS);
    }

    public void testOpenReadRetainsAdmissionAndCloseFailureReleasesBothPermits() throws Exception {
        AsyncExecutorContainer client = new AsyncExecutorContainer(Runnable::run, 1, 0, TimeValue.ZERO);
        var read = client.openReadStreamAsync(() -> new InputStreamContainer(new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() throws IOException {
                throw new IOException("close failed");
            }
        }, 0, 0)).get(30, TimeUnit.SECONDS);
        ExecutionException rejected = expectThrows(
            ExecutionException.class,
            () -> client.executeAsync(() -> null).get(30, TimeUnit.SECONDS)
        );
        assertTrue(rejected.getCause() instanceof RejectedExecutionException);
        assertTrue(rejected.getCause().getMessage().contains("outstanding"));
        expectThrows(IOException.class, () -> read.getInputStream().close());
        read.getInputStream().close();
        client.executeAsync(() -> null).get(30, TimeUnit.SECONDS);
    }
}
