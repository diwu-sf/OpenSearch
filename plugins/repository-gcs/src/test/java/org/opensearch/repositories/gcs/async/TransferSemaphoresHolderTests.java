/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.async;

import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

public class TransferSemaphoresHolderTests extends OpenSearchTestCase {
    public void testNormalBorrowsIdleLowAllocationAndReleasesOnlyOnce() throws Exception {
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(7, 3, TimeValue.ZERO);
        var context = holder.createRequestContext();
        List<TransferSemaphoresHolder.Permit> permits = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                permits.add(holder.acquirePermit(WritePriority.NORMAL, context));
            }
            expectThrows(RejectedExecutionException.class, () -> holder.acquirePermit(WritePriority.NORMAL, context));
            permits.remove(0).close();
            var replacement = holder.acquirePermit(WritePriority.NORMAL, context);
            replacement.close();
            replacement.close();
            permits.add(holder.acquirePermit(WritePriority.NORMAL, context));
            expectThrows(RejectedExecutionException.class, () -> holder.acquirePermit(WritePriority.NORMAL, context));
        } finally {
            permits.forEach(TransferSemaphoresHolder.Permit::close);
        }
    }

    public void testNormalDoesNotBorrowBusyLowAllocation() throws Exception {
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(7, 3, TimeValue.ZERO);
        try (var low = holder.acquirePermit(WritePriority.LOW, holder.createRequestContext())) {
            var context = holder.createRequestContext();
            List<TransferSemaphoresHolder.Permit> normal = new ArrayList<>();
            try {
                for (int i = 0; i < 7; i++) {
                    normal.add(holder.acquirePermit(WritePriority.NORMAL, context));
                }
                expectThrows(RejectedExecutionException.class, () -> holder.acquirePermit(WritePriority.NORMAL, context));
            } finally {
                normal.forEach(TransferSemaphoresHolder.Permit::close);
            }
        }
    }

    public void testLowPreservesNormalCapacityAndHighUrgentBypassPermits() throws Exception {
        TransferSemaphoresHolder holder = new TransferSemaphoresHolder(7, 3, TimeValue.ZERO);
        List<TransferSemaphoresHolder.Permit> permits = new ArrayList<>();
        var context = holder.createRequestContext();
        try {
            // Three LOW permits plus five borrowed NORMAL permits, leaving two NORMAL permits.
            for (int i = 0; i < 8; i++) {
                permits.add(holder.acquirePermit(WritePriority.LOW, context));
            }
            expectThrows(RejectedExecutionException.class, () -> holder.acquirePermit(WritePriority.LOW, context));
            for (int i = 0; i < 2; i++) {
                permits.add(holder.acquirePermit(WritePriority.NORMAL, context));
            }
            expectThrows(RejectedExecutionException.class, () -> holder.acquirePermit(WritePriority.NORMAL, context));
            holder.acquirePermit(WritePriority.HIGH, context).close();
            holder.acquirePermit(WritePriority.URGENT, context).close();
        } finally {
            permits.forEach(TransferSemaphoresHolder.Permit::close);
        }
    }
}
