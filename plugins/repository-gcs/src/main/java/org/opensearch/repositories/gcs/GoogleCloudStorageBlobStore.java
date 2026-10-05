/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.BatchResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.gcs.async.AsyncExecutorContainer;
import org.opensearch.repositories.gcs.async.AsyncPartsHandler;
import org.opensearch.secure_sm.AccessController;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

class GoogleCloudStorageBlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageBlobStore.class);

    // The recommended maximum size of a blob that should be uploaded in a single
    // request. Larger files should be uploaded over multiple requests (this is
    // called "resumable upload")
    // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
    public static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE;

    static {
        final String key = "opensearch.repository_gcs.large_blob_threshold_byte_size";
        final String largeBlobThresholdByteSizeProperty = System.getProperty(key);
        if (largeBlobThresholdByteSizeProperty == null) {
            LARGE_BLOB_THRESHOLD_BYTE_SIZE = Math.toIntExact(new ByteSizeValue(5, ByteSizeUnit.MB).getBytes());
        } else {
            final int largeBlobThresholdByteSize;
            try {
                largeBlobThresholdByteSize = Integer.parseInt(largeBlobThresholdByteSizeProperty);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse " + key + " having value [" + largeBlobThresholdByteSizeProperty + "]");
            }
            if (largeBlobThresholdByteSize <= 0) {
                throw new IllegalArgumentException(key + " must be positive but was [" + largeBlobThresholdByteSizeProperty + "]");
            }
            LARGE_BLOB_THRESHOLD_BYTE_SIZE = largeBlobThresholdByteSize;
        }
    }

    private final String bucketName;
    private final String clientName;
    private final String repositoryName;
    private final GoogleCloudStorageService storageService;
    private final GoogleCloudStorageOperationsStats stats;
    private final AsyncPartsHandler partsHandler;

    GoogleCloudStorageBlobStore(
        String bucketName,
        String clientName,
        String repositoryName,
        GoogleCloudStorageService storageService,
        int bufferSize
    ) {
        this.bucketName = bucketName;
        this.clientName = clientName;
        this.repositoryName = repositoryName;
        this.storageService = storageService;
        this.stats = new GoogleCloudStorageOperationsStats(bucketName);
        this.partsHandler = new AsyncPartsHandler(
            this::client,
            bufferSize,
            this::getLargeBlobThresholdInBytes,
            stats::trackPutOperation,
            stats::trackPostOperation
        );
    }

    private Storage client() throws IOException {
        return storageService.client(clientName, repositoryName, stats);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GoogleCloudStorageBlobContainer(path, this);
    }

    @Override
    public void close() {
        storageService.closeRepositoryClient(repositoryName);
    }

    /**
     * List blobs in the specific bucket under the specified path. The path root is removed.
     *
     * @param path base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetadata> listBlobs(String path) throws IOException {
        return listBlobsByPrefix(path, "");
    }

    /**
     * List all blobs in the specific bucket with names prefixed
     *
     * @param path
     *            base path of the blobs to list. This path is removed from the
     *            names of the blobs returned.
     * @param prefix prefix of the blobs to list.
     * @return a map of blob names and their metadata.
     */
    Map<String, BlobMetadata> listBlobsByPrefix(String path, String prefix) throws IOException {
        final String pathPrefix = buildKey(path, prefix);
        final MapBuilder<String, BlobMetadata> mapBuilder = MapBuilder.newMapBuilder();
        AccessController.doPrivilegedChecked(
            () -> client().list(bucketName, BlobListOption.currentDirectory(), BlobListOption.prefix(pathPrefix))
                .iterateAll()
                .forEach(blob -> {
                    assert blob.getName().startsWith(path);
                    if (blob.isDirectory() == false) {
                        final String suffixName = blob.getName().substring(path.length());
                        mapBuilder.put(suffixName, new PlainBlobMetadata(suffixName, blob.getSize()));
                    }
                })
        );
        return mapBuilder.immutableMap();
    }

    Map<String, BlobContainer> listChildren(BlobPath path) throws IOException {
        final String pathStr = path.buildAsString();
        final MapBuilder<String, BlobContainer> mapBuilder = MapBuilder.newMapBuilder();
        AccessController.doPrivilegedChecked(
            () -> client().list(bucketName, BlobListOption.currentDirectory(), BlobListOption.prefix(pathStr))
                .iterateAll()
                .forEach(blob -> {
                    if (blob.isDirectory()) {
                        assert blob.getName().startsWith(pathStr);
                        assert blob.getName().endsWith("/");
                        // Strip path prefix and trailing slash
                        final String suffixName = blob.getName().substring(pathStr.length(), blob.getName().length() - 1);
                        if (suffixName.isEmpty() == false) {
                            mapBuilder.put(suffixName, new GoogleCloudStorageBlobContainer(path.add(suffixName), this));
                        }
                    }
                })
        );
        return mapBuilder.immutableMap();
    }

    /**
     * Returns true if the blob exists in the specific bucket
     *
     * @param blobName name of the blob
     * @return true iff the blob exists
     */
    boolean blobExists(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucketName, blobName);
        final Blob blob = AccessController.doPrivilegedChecked(() -> client().get(blobId));
        return blob != null;
    }

    /**
     * Returns an {@link java.io.InputStream} for the given blob name
     *
     * @param blobName name of the blob
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(String blobName) throws IOException {
        return new GoogleCloudStorageRetryingInputStream(client(), BlobId.of(bucketName, blobName));
    }

    /**
     * Returns an {@link java.io.InputStream} for the given blob's position and length
     *
     * @param blobName name of the blob
     * @param position starting position to read from
     * @param length length of bytes to read
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return new GoogleCloudStorageRetryingInputStream(
                client(),
                BlobId.of(bucketName, blobName),
                position,
                Math.addExact(position, length - 1)
            );
        }
    }

    /**
     * Writes a blob in the specific bucket
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobName).build();
        partsHandler.writeBlob(blobInfo, inputStream, blobSize, failIfAlreadyExists, null);
    }

    GoogleCloudStorageAsyncClients asyncClients() {
        return storageService.asyncClients(clientName);
    }

    void asyncBlobUpload(AsyncExecutorContainer client, String blobName, WriteContext context, ActionListener<Void> listener) {
        try {
            notifyListener(
                storageService.asyncService()
                    .transferManager()
                    .uploadObject(client, context, () -> partsHandler.uploadObject(bucketName, blobName, context)),
                listener
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        executeAsync(() -> {
            Blob blob = AccessController.doPrivilegedChecked(() -> client().get(BlobId.of(bucketName, blobName)));
            if (blob == null) {
                throw new NoSuchFileException(blobName);
            }
            BlobId id = BlobId.of(bucketName, blobName, blob.getGeneration());
            long length = blob.getSize();
            ReadContext.StreamPartCreator part = () -> asyncClients().client()
                .openReadStreamAsync(() -> new InputStreamContainer(readBlob(id, length), length, 0));
            return new ReadContext.Builder(length, List.of(part)).metadata(blob.getMetadata()).build();
        }, listener);
    }

    InputStream readBlob(BlobId blobId, long length) throws IOException {
        return length == 0 ? new ByteArrayInputStream(new byte[0]) : new GoogleCloudStorageRetryingInputStream(client(), blobId);
    }

    <T> void executeAsync(CheckedSupplier<T, IOException> operation, ActionListener<T> listener) {
        final CompletableFuture<T> future;
        try {
            future = asyncClients().client().executeAsync(operation);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        notifyListener(future, listener);
    }

    private static <T> void notifyListener(CompletableFuture<T> future, ActionListener<T> listener) {
        future.whenComplete((result, failure) -> {
            if (failure == null) {
                listener.onResponse(result);
            } else {
                Throwable cause = failure instanceof CompletionException ? failure.getCause() : failure;
                listener.onFailure(cause instanceof Exception ? (Exception) cause : new RuntimeException(cause));
            }
        });
    }

    // non-static, package private for testing
    long getLargeBlobThresholdInBytes() {
        return LARGE_BLOB_THRESHOLD_BYTE_SIZE;
    }

    /**
     * Deletes the given path and all its children.
     *
     * @param pathStr Name of path to delete
     */
    DeleteResult deleteDirectory(String pathStr) throws IOException {
        return AccessController.doPrivilegedChecked(() -> {
            DeleteResult deleteResult = DeleteResult.ZERO;
            Page<Blob> page = client().list(bucketName, BlobListOption.prefix(pathStr));
            do {
                final Collection<String> blobsToDelete = new ArrayList<>();
                final AtomicLong blobsDeleted = new AtomicLong(0L);
                final AtomicLong bytesDeleted = new AtomicLong(0L);
                page.getValues().forEach(b -> {
                    blobsToDelete.add(b.getName());
                    blobsDeleted.incrementAndGet();
                    bytesDeleted.addAndGet(b.getSize());
                });
                deleteBlobsIgnoringIfNotExists(blobsToDelete);
                deleteResult = deleteResult.add(blobsDeleted.get(), bytesDeleted.get());
                page = page.getNextPage();
            } while (page != null);
            return deleteResult;
        });
    }

    /**
     * Deletes multiple blobs from the specific bucket using a batch request
     *
     * @param blobNames names of the blobs to delete
     */
    void deleteBlobsIgnoringIfNotExists(Collection<String> blobNames) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        final List<BlobId> blobIdsToDelete = blobNames.stream().map(blob -> BlobId.of(bucketName, blob)).collect(Collectors.toList());
        final List<BlobId> failedBlobs = Collections.synchronizedList(new ArrayList<>());
        try {
            AccessController.doPrivilegedChecked(() -> {
                final AtomicReference<StorageException> ioe = new AtomicReference<>();
                final StorageBatch batch = client().batch();
                for (BlobId blob : blobIdsToDelete) {
                    batch.delete(blob).notify(new BatchResult.Callback<Boolean, StorageException>() {
                        @Override
                        public void success(Boolean result) {}

                        @Override
                        public void error(StorageException exception) {
                            if (exception.getCode() != HTTP_NOT_FOUND) {
                                failedBlobs.add(blob);
                                if (ioe.compareAndSet(null, exception) == false) {
                                    ioe.get().addSuppressed(exception);
                                }
                            }
                        }
                    });
                }
                batch.submit();

                final StorageException exception = ioe.get();
                if (exception != null) {
                    throw exception;
                }
            });
        } catch (final Exception e) {
            throw new IOException("Exception when deleting blobs [" + failedBlobs + "]", e);
        }
        assert failedBlobs.isEmpty();
    }

    private static String buildKey(String keyPath, String s) {
        assert s != null;
        return keyPath + s;
    }

    @Override
    public Map<String, Long> stats() {
        return stats.toMap();
    }
}
