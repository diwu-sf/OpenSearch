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

package org.opensearch.repositories.gcs.async;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.StreamContext;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.io.Streams;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.Objects;
import java.util.function.LongSupplier;

import static java.net.HttpURLConnection.HTTP_GONE;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

/** GCS multipart and sequential resumable uploads, including checksum validation and session restart. */
@InternalApi
public final class AsyncPartsHandler {
    private static final Logger logger = LogManager.getLogger(AsyncPartsHandler.class);
    private final CheckedSupplier<Storage, IOException> client;
    private final int bufferSize;
    private final LongSupplier largeBlobThreshold;
    private final Runnable trackPut;
    private final Runnable trackPost;

    public AsyncPartsHandler(
        CheckedSupplier<Storage, IOException> client,
        int bufferSize,
        LongSupplier largeBlobThreshold,
        Runnable trackPut,
        Runnable trackPost
    ) {
        this.client = client;
        this.bufferSize = bufferSize;
        this.largeBlobThreshold = largeBlobThreshold;
        this.trackPut = trackPut;
        this.trackPost = trackPost;
    }

    public void uploadObject(String bucket, String blobName, WriteContext context) throws IOException {
        if (context.getFileSize() < 0) {
            throw new IllegalArgumentException("Upload size must be non-negative");
        }
        if (context.doRemoteDataIntegrityCheck()) {
            throw new IllegalArgumentException("GCS uploads require local CRC32 verification");
        }
        Objects.requireNonNull(context.getUploadFinalizer(), "Upload finalizer is required");
        StreamContext streams = context.getStreamProvider(Math.max(1L, context.getFileSize()));
        BlobInfo info = BlobInfo.newBuilder(bucket, blobName).setMetadata(context.getMetadata()).build();
        try (InputStream input = new GoogleCloudStorageMultiStreamInputStream(streams, context.getFileSize())) {
            writeBlob(info, input, context.getFileSize(), context.isFailIfAlreadyExists(), context.getUploadFinalizer());
        }
    }

    /** Shared by the existing synchronous entry point and the admitted async upload. */
    public void writeBlob(
        BlobInfo info,
        InputStream input,
        long size,
        boolean failIfAlreadyExists,
        CheckedConsumer<Boolean, IOException> finalizer
    ) throws IOException {
        if (size > largeBlobThreshold.getAsLong()) {
            writeBlobResumable(info, input, size, failIfAlreadyExists, finalizer);
        } else {
            writeBlobMultipart(info, input, size, failIfAlreadyExists, finalizer);
        }
    }

    /**
     * Uploads a blob using the "resumable upload" method (multiple requests, which
     * can be independently retried in case of failure, see
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
     * @param blobInfo the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     * @param size expected size of the blob to be written
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    private void writeBlobResumable(
        BlobInfo blobInfo,
        InputStream inputStream,
        long size,
        boolean failIfAlreadyExists,
        CheckedConsumer<Boolean, IOException> finalizer
    ) throws IOException {
        // We retry 410 GONE errors to cover the unlikely but possible scenario where a resumable upload session becomes broken and
        // needs to be restarted from scratch. Given how unlikely a 410 error should be according to SLAs we retry only twice.
        assert inputStream.markSupported();
        inputStream.mark(Integer.MAX_VALUE);
        final byte[] buffer = new byte[size < bufferSize ? Math.toIntExact(size) : bufferSize];
        StorageException storageException = null;
        final Storage.BlobWriteOption[] writeOptions = failIfAlreadyExists
            ? new Storage.BlobWriteOption[] { Storage.BlobWriteOption.doesNotExist() }
            : new Storage.BlobWriteOption[0];
        for (int retry = 0; retry < 3; ++retry) {
            try {
                final WriteChannel writeChannel = AccessController.doPrivilegedChecked(() -> client.get().writer(blobInfo, writeOptions));
                /*
                 * It is not enough to wrap the call to Streams#copy, we have to wrap the privileged calls too; this is because Streams#copy
                 * is in the stacktrace and is not granted the permissions needed to close and write the channel.
                 */
                OutputStream output = Channels.newOutputStream(new WritableByteChannel() {

                    @SuppressForbidden(reason = "channel is based on a socket")
                    @Override
                    public int write(final ByteBuffer src) throws IOException {
                        try {
                            return AccessController.doPrivilegedChecked(() -> writeChannel.write(src));
                        } catch (final IOException ioe) {
                            final StorageException storageException = (StorageException) ExceptionsHelper.unwrap(
                                ioe,
                                StorageException.class
                            );
                            if (storageException != null) {
                                throw storageException;
                            }
                            throw ioe;
                        }
                    }

                    @Override
                    public boolean isOpen() {
                        return writeChannel.isOpen();
                    }

                    @Override
                    public void close() throws IOException {
                        AccessController.doPrivilegedChecked(writeChannel::close);
                    }
                });
                // Closing a GCS WriteChannel commits the object. Async uploads validate before closing;
                // on failure an uncommitted session is left to expire rather than publishing a corrupt object.
                org.opensearch.common.util.io.Streams.copy(inputStream, output, buffer, finalizer == null);
                if (finalizer != null) {
                    finalizer.accept(true);
                    output.close();
                }
                // We don't track this operation on the http layer as
                // we do with the GET/LIST operations since this operations
                // can trigger multiple underlying http requests but only one
                // operation is billed.
                trackPut.run();
                return;
            } catch (final StorageException se) {
                final int errorCode = se.getCode();
                if (errorCode == HTTP_GONE) {
                    logger.warn(() -> new ParameterizedMessage("Retrying broken resumable upload session for blob {}", blobInfo), se);
                    storageException = ExceptionsHelper.useOrSuppress(storageException, se);
                    inputStream.reset();
                    continue;
                } else if (failIfAlreadyExists && errorCode == HTTP_PRECON_FAILED) {
                    throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
                }
                if (storageException != null) {
                    se.addSuppressed(storageException);
                }
                throw se;
            }
        }
        assert storageException != null;
        throw storageException;
    }

    /**
     * Uploads a blob using the "multipart upload" method (a single
     * 'multipart/related' request containing both data and metadata. The request is
     * gziped), see:
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/multipart-upload
     *  @param blobInfo the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     * @param blobSize the size
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    private void writeBlobMultipart(
        BlobInfo blobInfo,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        CheckedConsumer<Boolean, IOException> finalizer
    ) throws IOException {
        assert blobSize <= largeBlobThreshold.getAsLong() : "large blob uploads should use the resumable upload method";
        final byte[] buffer = new byte[Math.toIntExact(blobSize)];
        Streams.readFully(inputStream, buffer);
        if (finalizer != null) {
            // Consume EOF to validate the complete stream before issuing the object-creating request.
            if (inputStream.read() != -1) {
                throw new IOException("Upload stream exceeds expected length " + blobSize);
            }
            finalizer.accept(true);
        }
        try {
            final Storage.BlobTargetOption[] targetOptions = failIfAlreadyExists
                ? new Storage.BlobTargetOption[] { Storage.BlobTargetOption.doesNotExist() }
                : new Storage.BlobTargetOption[0];
            AccessController.doPrivilegedChecked(() -> client.get().create(blobInfo, buffer, targetOptions));
            // We don't track this operation on the http layer as
            // we do with the GET/LIST operations since this operations
            // can trigger multiple underlying http requests but only one
            // operation is billed.
            trackPost.run();
        } catch (final StorageException se) {
            if (failIfAlreadyExists && se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

}
