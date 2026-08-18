/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class RemoteStoreFileDownloaderTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Directory source;
    private Directory destination;
    private Directory secondDestination;
    private RemoteStoreFileDownloader fileDownloader;
    private Map<String, Integer> files = new HashMap<>();

    @Before
    public void setup() throws IOException {
        final int streamLimit = randomIntBetween(1, 20);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put("indices.recovery.max_concurrent_remote_store_streams", streamLimit).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        threadPool = new TestThreadPool(getTestName());
        source = new NIOFSDirectory(createTempDir());
        destination = new NIOFSDirectory(createTempDir());
        secondDestination = new NIOFSDirectory(createTempDir());
        for (int i = 0; i < 10; i++) {
            final String filename = "file_" + i;
            final int content = randomInt();
            try (IndexOutput output = source.createOutput(filename, IOContext.DEFAULT)) {
                output.writeInt(content);
            }
            files.put(filename, content);
        }
        fileDownloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            recoverySettings
        );
    }

    @After
    public void stopThreadPool() throws Exception {
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testDownload() throws IOException {
        final PlainActionFuture<Void> l = new PlainActionFuture<>();
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, files.keySet(), l);
        l.actionGet();
        assertContent(files, destination);
    }

    public void testDownloadWithSecondDestination() throws IOException, InterruptedException {
        fileDownloader.download(source, destination, secondDestination, files.keySet(), () -> {});
        assertContent(files, destination);
        assertContent(files, secondDestination);
    }

    public void testDownloadWithFileCompletionHandler() throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        fileDownloader.download(source, destination, null, files.keySet(), counter::incrementAndGet);
        assertContent(files, destination);
        assertEquals(files.size(), counter.get());
    }

    /**
     * A blob container that records how many blobs were written byte-by-byte versus copied on the "server" side, so a
     * test can tell which of the two paths the downloader actually took.
     */
    private static class CountingFsBlobContainer extends FsBlobContainer {
        final AtomicInteger writeBlobCount = new AtomicInteger();
        final AtomicInteger copyBlobCount = new AtomicInteger();

        CountingFsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
            super(blobStore, blobPath, path);
        }

        @Override
        public void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            writeBlobCount.incrementAndGet();
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void copyBlob(BlobContainer sourceBlobContainer, String sourceBlobName, String blobName, long blobSize) throws IOException {
            copyBlobCount.incrementAndGet();
            super.copyBlob(sourceBlobContainer, sourceBlobName, blobName, blobSize);
        }
    }

    private RemoteSegmentStoreDirectory newRemoteSegmentStoreDirectory(BlobContainer blobContainer) throws IOException {
        return new RemoteSegmentStoreDirectory(
            new RemoteDirectory(blobContainer),
            mock(RemoteDirectory.class),
            mock(RemoteStoreMetadataLockManager.class),
            threadPool,
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            new HashMap<>()
        );
    }

    private RemoteStoreSettings remoteStoreSettings(boolean serverSideCopyEnabled) {
        return new RemoteStoreSettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENT_SERVER_SIDE_COPY_ENABLED.getKey(), serverSideCopyEnabled)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    /**
     * Writes a handful of well-formed Lucene files (the remote segment store computes a codec checksum on upload, so
     * they need a real footer) into a fresh local directory and seeds a remote segment store directory from them.
     */
    private Set<String> seedSourceRemoteDirectory(Directory localSource, RemoteSegmentStoreDirectory sourceRemote) throws IOException {
        final Set<String> filenames = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            final String filename = "_" + i + ".si";
            try (IndexOutput output = localSource.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("segment content " + randomAlphaOfLength(20));
                CodecUtil.writeFooter(output);
            }
            localSource.sync(Set.of(filename));
            sourceRemote.copyFrom(localSource, filename, filename, IOContext.DEFAULT);
            filenames.add(filename);
        }
        return filenames;
    }

    private void assertSameContent(Directory expected, Directory actual, Set<String> filenames) throws IOException {
        for (String filename : filenames) {
            assertEquals(expected.fileLength(filename), actual.fileLength(filename));
            final byte[] expectedBytes = new byte[(int) expected.fileLength(filename)];
            final byte[] actualBytes = new byte[expectedBytes.length];
            try (IndexInput in = expected.openInput(filename, IOContext.DEFAULT)) {
                in.readBytes(expectedBytes, 0, expectedBytes.length);
            }
            try (IndexInput in = actual.openInput(filename, IOContext.DEFAULT)) {
                in.readBytes(actualBytes, 0, actualBytes.length);
            }
            assertArrayEquals(expectedBytes, actualBytes);
        }
    }

    public void testServerSideCopySkipsUploadLeg() throws Exception {
        final Path sourceBlobPath = createTempDir();
        final Path targetBlobPath = createTempDir();
        final CountingFsBlobContainer sourceContainer = new CountingFsBlobContainer(
            new FsBlobStore(1024, sourceBlobPath, false),
            BlobPath.cleanPath(),
            sourceBlobPath
        );
        final CountingFsBlobContainer targetContainer = new CountingFsBlobContainer(
            new FsBlobStore(1024, targetBlobPath, false),
            BlobPath.cleanPath(),
            targetBlobPath
        );

        final Directory localSource = new NIOFSDirectory(createTempDir());
        final RemoteSegmentStoreDirectory sourceRemote = newRemoteSegmentStoreDirectory(sourceContainer);
        final Set<String> filenames = seedSourceRemoteDirectory(localSource, sourceRemote);
        final RemoteSegmentStoreDirectory targetRemote = newRemoteSegmentStoreDirectory(targetContainer);
        targetContainer.writeBlobCount.set(0);

        final RemoteStoreFileDownloader downloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            remoteStoreSettings(true)
        );

        downloader.download(sourceRemote, destination, targetRemote, filenames, () -> {});

        // The local store is still populated, because the engine needs it to open.
        assertSameContent(localSource, destination, filenames);
        // Every file was copied on the "server" side and none was uploaded back from local disk.
        assertEquals(filenames.size(), targetContainer.copyBlobCount.get());
        assertEquals(0, targetContainer.writeBlobCount.get());
        // And the target directory knows about all of them, with the source's checksums.
        for (String filename : filenames) {
            assertEquals(
                sourceRemote.getSegmentsUploadedToRemoteStore().get(filename).getChecksum(),
                targetRemote.getSegmentsUploadedToRemoteStore().get(filename).getChecksum()
            );
        }
    }

    public void testUploadLegStillRunsWhenServerSideCopyDisabled() throws Exception {
        final Path sourceBlobPath = createTempDir();
        final Path targetBlobPath = createTempDir();
        final CountingFsBlobContainer sourceContainer = new CountingFsBlobContainer(
            new FsBlobStore(1024, sourceBlobPath, false),
            BlobPath.cleanPath(),
            sourceBlobPath
        );
        final CountingFsBlobContainer targetContainer = new CountingFsBlobContainer(
            new FsBlobStore(1024, targetBlobPath, false),
            BlobPath.cleanPath(),
            targetBlobPath
        );

        final Directory localSource = new NIOFSDirectory(createTempDir());
        final RemoteSegmentStoreDirectory sourceRemote = newRemoteSegmentStoreDirectory(sourceContainer);
        final Set<String> filenames = seedSourceRemoteDirectory(localSource, sourceRemote);
        final RemoteSegmentStoreDirectory targetRemote = newRemoteSegmentStoreDirectory(targetContainer);
        targetContainer.writeBlobCount.set(0);

        final RemoteStoreFileDownloader downloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            remoteStoreSettings(false)
        );

        downloader.download(sourceRemote, destination, targetRemote, filenames, () -> {});

        assertSameContent(localSource, destination, filenames);
        // With the optimization off we get exactly the old behaviour: upload from local, no server side copy.
        assertEquals(0, targetContainer.copyBlobCount.get());
        assertEquals(filenames.size(), targetContainer.writeBlobCount.get());
        for (String filename : filenames) {
            assertTrue(targetRemote.getSegmentsUploadedToRemoteStore().containsKey(filename));
        }
    }

    public void testDownloadNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, Set.of("not real"), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(NoSuchFileException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testDownloadExtraNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> filesWithExtra = new ArrayList<>(files.keySet());
        filesWithExtra.add("not real");
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, filesWithExtra, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(NoSuchFileException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testCancellable() {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> blockingListener = new PlainActionFuture<>();
        final Directory blockingDestination = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                try {
                    Thread.sleep(60_000); // Will be interrupted
                    fail("Expected to be interrupted");
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed due to interrupt", e);
                }
            }
        };
        fileDownloader.downloadAsync(cancellableThreads, source, blockingDestination, files.keySet(), blockingListener);
        assertThrows(
            "Expected to timeout due to blocking directory",
            OpenSearchTimeoutException.class,
            () -> blockingListener.actionGet(TimeValue.timeValueMillis(500))
        );
        cancellableThreads.cancel("test");
        assertThrows(
            "Expected to complete with cancellation failure",
            CancellableThreads.ExecutionCancelledException.class,
            blockingListener::actionGet
        );
    }

    public void testBlockingCallCanBeInterrupted() throws Exception {
        final Directory blockingDestination = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                try {
                    Thread.sleep(60_000); // Will be interrupted
                    fail("Expected to be interrupted");
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed due to interrupt", e);
                }
            }
        };
        final AtomicReference<Exception> capturedException = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                fileDownloader.download(source, blockingDestination, null, files.keySet(), () -> {});
            } catch (Exception e) {
                capturedException.set(e);
            }
        });
        thread.start();
        thread.interrupt();
        thread.join();
        assertEquals(InterruptedException.class, capturedException.get().getClass());
    }

    public void testIOException() throws IOException, InterruptedException {
        final Directory failureDirectory = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
                throw new IOException("test");
            }
        };
        assertThrows(IOException.class, () -> fileDownloader.download(source, failureDirectory, null, files.keySet(), () -> {}));

        final CountDownLatch latch = new CountDownLatch(1);
        fileDownloader.downloadAsync(new CancellableThreads(), source, failureDirectory, files.keySet(), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(IOException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void assertContent(Map<String, Integer> expected, Directory destination) throws IOException {
        // Note that Lucene will randomly write extra files (see org.apache.lucene.tests.mockfile.ExtraFS)
        // so we just need to check that all the expected files are present but not that _only_ the expected
        // files are present
        final Set<String> actualFiles = Set.of(destination.listAll());
        for (String file : expected.keySet()) {
            assertTrue(actualFiles.contains(file));
            try (IndexInput input = destination.openInput(file, IOContext.DEFAULT)) {
                assertEquals(expected.get(file), Integer.valueOf(input.readInt()));
                assertThrows(EOFException.class, input::readByte);
            }
        }
    }
}
