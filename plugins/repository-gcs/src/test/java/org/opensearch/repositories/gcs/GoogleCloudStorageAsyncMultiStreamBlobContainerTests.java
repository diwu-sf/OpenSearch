/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageAsyncMultiStreamBlobContainerTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private Storage storage;
    private GoogleCloudStorageService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName(), GoogleCloudStoragePlugin.asyncExecutorBuilder());
        storage = mock(Storage.class);
        service = new GoogleCloudStorageService() {
            @Override
            public Storage client(String client, String repository, GoogleCloudStorageOperationsStats stats) {
                return storage;
            }
        };
        service.setAsyncExecutor(threadPool.executor(GoogleCloudStoragePlugin.ASYNC_TRANSFER));
    }

    @Override
    public void tearDown() throws Exception {
        try {
            terminate(threadPool);
        } finally {
            super.tearDown();
        }
    }

    public void testConcurrencyIsBoundedAcrossRepositories() throws Exception {
        int concurrency = GoogleCloudStoragePlugin.MAX_CONCURRENT_OPERATIONS;
        CountDownLatch started = new CountDownLatch(concurrency);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        threadPool.getThreadContext().putHeader("test-upload", "preserved");
        when(storage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class))).thenAnswer(invocation -> {
            assertTrue(Thread.currentThread().isVirtual());
            assertEquals("preserved", threadPool.getThreadContext().getHeader("test-upload"));
            assertArrayEquals(new byte[] { 1, 2, 3 }, invocation.getArgument(1));
            maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
            started.countDown();
            try {
                assertTrue(release.await(30, TimeUnit.SECONDS));
            } finally {
                active.decrementAndGet();
            }
            completed.incrementAndGet();
            return null;
        });

        List<CompletableFuture<Void>> results = new ArrayList<>();
        try (
            GoogleCloudStorageBlobStore first = new GoogleCloudStorageBlobStore("bucket", "default", "first", service, 1024);
            GoogleCloudStorageBlobStore second = new GoogleCloudStorageBlobStore("bucket", "default", "second", service, 1024)
        ) {
            AsyncMultiStreamBlobContainer firstContainer = (AsyncMultiStreamBlobContainer) first.blobContainer(
                BlobPath.cleanPath().add("first")
            );
            AsyncMultiStreamBlobContainer secondContainer = (AsyncMultiStreamBlobContainer) second.blobContainer(
                BlobPath.cleanPath().add("second")
            );
            try {
                for (int i = 0; i < concurrency * 2; i++) {
                    results.add(upload(i % 2 == 0 ? firstContainer : secondContainer, "blob-" + i));
                }
                assertTrue(started.await(30, TimeUnit.SECONDS));
                assertEquals(concurrency, active.get());
                assertEquals(concurrency, maximum.get());
                assertEquals(0, completed.get());
                assertTrue(results.stream().noneMatch(CompletableFuture::isDone));
            } finally {
                release.countDown();
            }
            CompletableFuture.allOf(results.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
            assertEquals(concurrency * 2, completed.get());
            assertEquals(concurrency, maximum.get());
        }
    }

    public void testUploadFailureNotifiesListener() throws Exception {
        StorageException failure = new StorageException(403, "denied");
        when(storage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class))).thenThrow(failure);
        try (GoogleCloudStorageBlobStore store = new GoogleCloudStorageBlobStore("bucket", "default", "repo", service, 1024)) {
            CompletableFuture<Void> result = upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), "blob");
            ExecutionException exception = expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS));
            assertSame(failure, exception.getCause());
        }
    }

    public void testRejectedUploadNotifiesListener() throws Exception {
        RejectedExecutionException failure = new RejectedExecutionException("full");
        service.setAsyncExecutor(command -> { throw failure; });
        try (GoogleCloudStorageBlobStore store = new GoogleCloudStorageBlobStore("bucket", "default", "repo", service, 1024)) {
            CompletableFuture<Void> result = upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), "blob");
            ExecutionException exception = expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS));
            assertSame(failure, exception.getCause());
            verifyNoInteractions(storage);
        }
    }

    public void testPendingUploadsAreBoundedAndCapacityIsReleasedAfterFailure() throws Exception {
        List<Runnable> pending = new ArrayList<>();
        service.setAsyncExecutor(pending::add);
        int capacity = GoogleCloudStoragePlugin.MAX_CONCURRENT_OPERATIONS + GoogleCloudStorageService.MAX_PENDING_OPERATIONS;
        List<CompletableFuture<Void>> results = new ArrayList<>();
        IOException failure = new IOException("upload failed");
        for (int i = 0; i < capacity; i++) {
            results.add(service.executeAsync(() -> { throw failure; }));
        }
        assertTrue(
            expectThrows(ExecutionException.class, () -> service.executeAsync(() -> null).get(30, TimeUnit.SECONDS))
                .getCause() instanceof RejectedExecutionException
        );
        assertEquals(capacity, pending.size());
        pending.forEach(Runnable::run);
        for (CompletableFuture<Void> result : results) {
            assertSame(failure, expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS)).getCause());
        }
        pending.clear();
        for (int i = 0; i < capacity; i++) {
            service.executeAsync(() -> null);
        }
        assertEquals(capacity, pending.size());
        pending.forEach(Runnable::run);
    }

    public void testInterruptedUploadNotifiesListener() throws Exception {
        service.setAsyncExecutor(command -> threadPool.executor(GoogleCloudStoragePlugin.ASYNC_TRANSFER).execute(() -> {
            Thread.currentThread().interrupt();
            command.run();
            assertTrue(Thread.interrupted());
        }));
        try (GoogleCloudStorageBlobStore store = new GoogleCloudStorageBlobStore("bucket", "default", "repo", service, 1024)) {
            CompletableFuture<Void> result = upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), "blob");
            ExecutionException exception = expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS));
            assertTrue(exception.getCause() instanceof InterruptedException);
            verifyNoInteractions(storage);
        }
    }

    public void testMultipartStreamsAndMetadata() throws Exception {
        byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
        AtomicInteger closed = new AtomicInteger();
        AtomicBoolean finalized = new AtomicBoolean();
        when(storage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class))).thenAnswer(invocation -> {
            assertTrue(finalized.get());
            assertArrayEquals(bytes, invocation.getArgument(1));
            BlobInfo info = invocation.getArgument(0);
            assertEquals("path/blob", info.getName());
            assertEquals(Map.of("checkpoint", "value"), info.getMetadata());
            return null;
        });
        try (GoogleCloudStorageBlobStore store = store()) {
            WriteContext context = context("blob", bytes, closed, success -> finalized.set(success), 3, true);
            upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath().add("path")), context).get(
                30,
                TimeUnit.SECONDS
            );
            assertEquals(2, closed.get());
            assertFalse(((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath())).remoteIntegrityCheckSupported());
        }
    }

    public void testEmptyBlob() throws Exception {
        try (GoogleCloudStorageBlobStore store = store()) {
            WriteContext context = context("empty", new byte[0], new AtomicInteger(), success -> {}, 1, true);
            upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), context).get(30, TimeUnit.SECONDS);
            verify(storage).create(any(BlobInfo.class), eq(new byte[0]), any(Storage.BlobTargetOption[].class));
        }
    }

    public void testChecksumFailureDoesNotCreateBlob() throws Exception {
        IOException failure = new IOException("checksum mismatch");
        AtomicInteger closed = new AtomicInteger();
        try (GoogleCloudStorageBlobStore store = store()) {
            WriteContext context = context("corrupt", new byte[] { 1, 2 }, closed, success -> { throw failure; }, 2, true);
            ExecutionException exception = expectThrows(
                ExecutionException.class,
                () -> upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), context).get(30, TimeUnit.SECONDS)
            );
            assertSame(failure, exception.getCause());
            verifyNoInteractions(storage);
            assertEquals(1, closed.get());
        }
    }

    public void testTruncatedStreamDoesNotCreateBlob() throws Exception {
        assertInvalidStreamLength(4);
    }

    public void testOversizedStreamDoesNotCreateBlob() throws Exception {
        assertInvalidStreamLength(2);
    }

    private void assertInvalidStreamLength(long expectedLength) throws Exception {
        AtomicInteger closed = new AtomicInteger();
        AtomicBoolean finalized = new AtomicBoolean();
        WriteContext source = context("blob", new byte[] { 1, 2, 3 }, closed, finalized::set, 3, true);
        WriteContext invalid = new WriteContext.Builder().fileName("blob")
            .fileSize(expectedLength)
            .uploadFinalizer(source.getUploadFinalizer())
            .streamContextSupplier(source::getStreamProvider)
            .build();
        try (GoogleCloudStorageBlobStore store = store()) {
            ExecutionException failure = expectThrows(
                ExecutionException.class,
                () -> upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), invalid).get(30, TimeUnit.SECONDS)
            );
            assertTrue(failure.getCause() instanceof IOException);
            assertFalse(finalized.get());
            assertEquals(1, closed.get());
            verifyNoInteractions(storage);
        }
    }

    public void testResumableUploadStreamsAndFinalizesBeforeCommit() throws Exception {
        testResumableUpload(false);
    }

    public void testExpiredResumableSessionReopensStreams() throws Exception {
        testResumableUpload(true);
    }

    private void testResumableUpload(boolean expireSession) throws Exception {
        byte[] bytes = randomByteArrayOfLength(23);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AtomicBoolean finalized = new AtomicBoolean();
        AtomicInteger closed = new AtomicInteger();
        WriteChannel channel = mock(WriteChannel.class);
        when(channel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            int length = buffer.remaining();
            byte[] part = new byte[length];
            buffer.get(part);
            output.write(part);
            return length;
        });
        org.mockito.Mockito.doAnswer(invocation -> {
            assertTrue(finalized.get());
            assertArrayEquals(bytes, output.toByteArray());
            return null;
        }).when(channel).close();
        if (expireSession) {
            WriteChannel expired = mock(WriteChannel.class);
            when(expired.write(any(ByteBuffer.class))).thenThrow(new StorageException(410, "expired"));
            when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption[].class))).thenReturn(expired, channel);
        } else {
            when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption[].class))).thenReturn(channel);
        }
        try (GoogleCloudStorageBlobStore store = streamingStore()) {
            WriteContext context = context("large", bytes, closed, success -> finalized.set(success), 7, true);
            upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), context).get(30, TimeUnit.SECONDS);
            verify(channel).close();
            assertEquals(expireSession ? 5 : 4, closed.get());
            verify(storage, never()).create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class));
        }
    }

    public void testResumableChecksumFailureDoesNotCommit() throws Exception {
        WriteChannel channel = mock(WriteChannel.class);
        when(channel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            int size = buffer.remaining();
            buffer.position(buffer.limit());
            return size;
        });
        when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption[].class))).thenReturn(channel);
        try (GoogleCloudStorageBlobStore store = streamingStore()) {
            WriteContext context = context(
                "corrupt",
                new byte[12],
                new AtomicInteger(),
                success -> { throw new IOException("checksum mismatch"); },
                12,
                true
            );
            expectThrows(
                ExecutionException.class,
                () -> upload((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath()), context).get(30, TimeUnit.SECONDS)
            );
            verify(channel, never()).close();
        }
    }

    public void testAsyncReadPinsGenerationAndPreservesMetadata() throws Exception {
        Blob blob = mock(Blob.class);
        when(blob.getGeneration()).thenReturn(42L);
        when(blob.getSize()).thenReturn(3L);
        when(blob.getMetadata()).thenReturn(Map.of("checkpoint", "data"));
        when(storage.get(BlobId.of("bucket", "path/blob"))).thenReturn(blob);
        try (GoogleCloudStorageBlobStore store = spy(store())) {
            doReturn(new ByteArrayInputStream(new byte[] { 1, 2, 3 })).when(store).readBlob(BlobId.of("bucket", "path/blob", 42L), 3L);
            CompletableFuture<ReadContext> result = new CompletableFuture<>();
            ((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath().add("path"))).readBlobAsync(
                "blob",
                ActionListener.wrap(result::complete, result::completeExceptionally)
            );
            ReadContext context = result.get(30, TimeUnit.SECONDS);
            assertEquals(3, context.getBlobSize());
            assertEquals(Map.of("checkpoint", "data"), context.getMetadata());
            assertNull(context.getBlobChecksum());
            InputStreamContainer part = context.getPartStreams().get(0).get().get(30, TimeUnit.SECONDS);
            try (InputStream input = part.getInputStream()) {
                assertArrayEquals(new byte[] { 1, 2, 3 }, input.readAllBytes());
            }
            verify(store).readBlob(BlobId.of("bucket", "path/blob", 42L), 3L);
        }
    }

    public void testMissingAsyncReadNotifiesListener() throws Exception {
        try (GoogleCloudStorageBlobStore store = store()) {
            CompletableFuture<ReadContext> result = new CompletableFuture<>();
            ((AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath())).readBlobAsync(
                "missing",
                ActionListener.wrap(result::complete, result::completeExceptionally)
            );
            assertTrue(
                expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS)).getCause() instanceof NoSuchFileException
            );
        }
    }

    public void testReadStreamHoldsPermitUntilClose() throws Exception {
        List<InputStream> streams = new ArrayList<>();
        try {
            for (int i = 0; i < GoogleCloudStoragePlugin.MAX_CONCURRENT_OPERATIONS; i++) {
                streams.add(
                    service.openReadStreamAsync(() -> new InputStreamContainer(new ByteArrayInputStream(new byte[0]), 0, 0))
                        .get(30, TimeUnit.SECONDS)
                        .getInputStream()
                );
            }
            CompletableFuture<Void> upload = service.executeAsync(() -> null);
            assertFalse(upload.isDone());
            InputStream released = streams.remove(0);
            released.close();
            released.close();
            upload.get(30, TimeUnit.SECONDS);
        } finally {
            for (InputStream input : streams) {
                input.close();
            }
        }
    }

    public void testCancelledReadClosesStreamAndReleasesCapacity() throws Exception {
        List<Runnable> queued = new ArrayList<>();
        service.setAsyncExecutor(queued::add);
        AtomicInteger closed = new AtomicInteger();
        CompletableFuture<InputStreamContainer> result = service.openReadStreamAsync(
            () -> new InputStreamContainer(new ByteArrayInputStream(new byte[0]) {
                @Override
                public void close() {
                    closed.incrementAndGet();
                }
            }, 0, 0)
        );
        assertTrue(result.cancel(false));
        queued.remove(0).run();
        assertEquals(1, closed.get());
        service.setAsyncExecutor(threadPool.executor(GoogleCloudStoragePlugin.ASYNC_TRANSFER));
        testReadStreamHoldsPermitUntilClose();
    }

    public void testFailedReadReleasesCapacity() throws Exception {
        IOException failure = new IOException("read failed");
        for (int i = 0; i < GoogleCloudStoragePlugin.MAX_CONCURRENT_OPERATIONS; i++) {
            CompletableFuture<InputStreamContainer> result = service.openReadStreamAsync(() -> { throw failure; });
            assertSame(failure, expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS)).getCause());
        }
        testReadStreamHoldsPermitUntilClose();
    }

    public void testAsyncDeletesUseVirtualThreadsAndPreservePaths() throws Exception {
        AtomicReference<List<String>> deleted = new AtomicReference<>();
        try (GoogleCloudStorageBlobStore store = new GoogleCloudStorageBlobStore("bucket", "default", "repo", service, 1024) {
            @Override
            void deleteBlobsIgnoringIfNotExists(Collection<String> names) {
                assertTrue(Thread.currentThread().isVirtual());
                deleted.set(List.copyOf(names));
            }

            @Override
            DeleteResult deleteDirectory(String path) {
                assertTrue(Thread.currentThread().isVirtual());
                assertEquals("path/", path);
                return new DeleteResult(2, 17);
            }
        }) {
            AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) store.blobContainer(BlobPath.cleanPath().add("path"));
            CompletableFuture<Void> result = new CompletableFuture<>();
            container.deleteBlobsAsyncIgnoringIfNotExists(
                List.of("a", "b"),
                ActionListener.wrap(result::complete, result::completeExceptionally)
            );
            result.get(30, TimeUnit.SECONDS);
            assertEquals(List.of("path/a", "path/b"), deleted.get());
            CompletableFuture<DeleteResult> directory = new CompletableFuture<>();
            container.deleteAsync(ActionListener.wrap(directory::complete, directory::completeExceptionally));
            assertEquals(2, directory.get(30, TimeUnit.SECONDS).blobsDeleted());
        }
    }

    public void testRemoteSegmentCallerUsesAsyncUpload() throws Exception {
        Path path = createTempDir();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (FSDirectory source = FSDirectory.open(path); GoogleCloudStorageBlobStore store = store()) {
            try (IndexOutput output = source.createOutput("segment", IOContext.DEFAULT)) {
                byte[] bytes = randomByteArrayOfLength(8192);
                output.writeBytes(bytes, bytes.length);
                CodecUtil.writeFooter(output);
            }
            byte[] expected = Files.readAllBytes(path.resolve("segment"));
            when(storage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class))).thenAnswer(invocation -> {
                assertTrue(Thread.currentThread().isVirtual());
                assertArrayEquals(expected, invocation.getArgument(1));
                entered.countDown();
                assertTrue(release.await(30, TimeUnit.SECONDS));
                return null;
            });
            try (RemoteDirectory remote = new RemoteDirectory(store.blobContainer(BlobPath.cleanPath()))) {
                CompletableFuture<Void> result = new CompletableFuture<>();
                try {
                    assertTrue(
                        remote.copyFrom(
                            source,
                            "segment",
                            "remote-segment",
                            IOContext.DEFAULT,
                            () -> {},
                            ActionListener.wrap(result::complete, result::completeExceptionally),
                            false,
                            null
                        )
                    );
                    assertTrue(entered.await(30, TimeUnit.SECONDS));
                    assertFalse(result.isDone());
                } finally {
                    release.countDown();
                }
                result.get(30, TimeUnit.SECONDS);
            }
        }
    }

    public void testFileBackedTranslogCallerUsesAsyncUpload() throws Exception {
        byte[] bytes = randomByteArrayOfLength(8192);
        Path path = createTempDir().resolve("translog-2.tlog");
        Files.write(path, bytes);
        CRC32 checksum = new CRC32();
        checksum.update(bytes);
        when(storage.create(any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class))).thenAnswer(invocation -> {
            assertTrue(Thread.currentThread().isVirtual());
            assertArrayEquals(bytes, invocation.getArgument(1));
            return null;
        });
        try (
            GoogleCloudStorageBlobStore store = store();
            TransferFileSnapshot snapshot = new TransferFileSnapshot(path, 1, checksum.getValue())
        ) {
            BlobStoreTransferService transfers = new BlobStoreTransferService(store, threadPool);
            CompletableFuture<TransferFileSnapshot> result = new CompletableFuture<>();
            transfers.uploadBlobs(
                Set.of(snapshot),
                Map.of(1L, BlobPath.cleanPath()),
                ActionListener.wrap(result::complete, result::completeExceptionally),
                WritePriority.HIGH,
                null
            );
            assertSame(snapshot, result.get(30, TimeUnit.SECONDS));
        }
    }

    public void testCorruptTranslogDoesNotCreateObject() throws Exception {
        byte[] bytes = randomByteArrayOfLength(128);
        Path path = createTempDir().resolve("translog-2.tlog");
        Files.write(path, bytes);
        CRC32 checksum = new CRC32();
        checksum.update(bytes);
        try (
            GoogleCloudStorageBlobStore store = store();
            TransferFileSnapshot snapshot = new TransferFileSnapshot(path, 1, checksum.getValue() ^ 1)
        ) {
            BlobStoreTransferService transfers = new BlobStoreTransferService(store, threadPool);
            CompletableFuture<TransferFileSnapshot> result = new CompletableFuture<>();
            transfers.uploadBlobs(
                Set.of(snapshot),
                Map.of(1L, BlobPath.cleanPath()),
                ActionListener.wrap(result::complete, result::completeExceptionally),
                WritePriority.HIGH,
                null
            );
            ExecutionException failure = expectThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS));
            assertNotNull(org.opensearch.ExceptionsHelper.unwrapCorruption(failure));
            verifyNoInteractions(storage);
        }
    }

    private GoogleCloudStorageBlobStore store() {
        return new GoogleCloudStorageBlobStore("bucket", "default", "repo", service, 1024);
    }

    private GoogleCloudStorageBlobStore streamingStore() {
        return new GoogleCloudStorageBlobStore("bucket", "default", "repo", service, 5) {
            @Override
            long getLargeBlobThresholdInBytes() {
                return 4;
            }
        };
    }

    private WriteContext context(
        String name,
        byte[] bytes,
        AtomicInteger closed,
        CheckedConsumer<Boolean, IOException> finalizer,
        int partSize,
        boolean failIfExists
    ) {
        int parts = Math.max(1, (bytes.length + partSize - 1) / partSize);
        return new WriteContext.Builder().fileName(name)
            .fileSize(bytes.length)
            .failIfAlreadyExists(failIfExists)
            .writePriority(WritePriority.NORMAL)
            .uploadFinalizer(finalizer)
            .metadata(Map.of("checkpoint", "value"))
            .streamContextSupplier(
                ignored -> new StreamContext(
                    (part, size, position) -> new InputStreamContainer(
                        new ByteArrayInputStream(bytes, Math.toIntExact(position), Math.toIntExact(size)) {
                            @Override
                            public void close() {
                                closed.incrementAndGet();
                            }
                        },
                        size,
                        position
                    ),
                    partSize,
                    bytes.length - (long) (parts - 1) * partSize,
                    parts
                )
            )
            .build();
    }

    private CompletableFuture<Void> upload(AsyncMultiStreamBlobContainer container, String name) {
        return upload(container, context(name, new byte[] { 1, 2, 3 }, new AtomicInteger(), success -> {}, 3, true));
    }

    private CompletableFuture<Void> upload(AsyncMultiStreamBlobContainer container, WriteContext context) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            container.asyncBlobUpload(context, ActionListener.wrap(result::complete, result::completeExceptionally));
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
        return result;
    }
}
