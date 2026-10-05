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

import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.gcs.async.AsyncExecutorContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class GoogleCloudStorageBlobContainer extends AbstractBlobContainer implements AsyncMultiStreamBlobContainer {

    private final GoogleCloudStorageBlobStore blobStore;
    private final String path;

    GoogleCloudStorageBlobContainer(BlobPath path, GoogleCloudStorageBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.path = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return blobStore.listBlobs(path);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return blobStore.listChildren(path());
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String prefix) throws IOException {
        return blobStore.listBlobsByPrefix(path, prefix);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return blobStore.readBlob(buildKey(blobName));
    }

    @Override
    public InputStream readBlob(final String blobName, final long position, final long length) throws IOException {
        return blobStore.readBlob(buildKey(blobName), position, length);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void asyncBlobUpload(WriteContext context, ActionListener<Void> listener) {
        try {
            GoogleCloudStorageAsyncClients clients = blobStore.asyncClients();
            AsyncExecutorContainer client;
            if (context.getWritePriority() == WritePriority.URGENT) {
                client = clients.urgentClient();
            } else if (context.getWritePriority() == WritePriority.HIGH) {
                client = clients.priorityClient();
            } else {
                client = clients.client();
            }
            blobStore.asyncBlobUpload(client, buildKey(context.getFileName()), context, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public boolean remoteIntegrityCheckSupported() {
        // OpenSearch uses CRC32; the GCS object checksum is CRC32C. Use the local upload finalizer.
        return false;
    }

    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        blobStore.readBlobAsync(buildKey(blobName), listener);
    }

    @Override
    public void deleteAsync(ActionListener<DeleteResult> listener) {
        blobStore.executeAsync(this::delete, listener);
    }

    @Override
    public void deleteBlobsAsyncIgnoringIfNotExists(List<String> blobNames, ActionListener<Void> listener) {
        List<String> names = List.copyOf(blobNames);
        blobStore.executeAsync(() -> {
            deleteBlobsIgnoringIfNotExists(names);
            return null;
        }, listener);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata
    ) throws IOException {
        // Preserve the existing synchronous behavior, which does not store custom metadata.
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return blobStore.deleteDirectory(path().buildAsString());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        blobStore.deleteBlobsIgnoringIfNotExists(blobNames.stream().map(this::buildKey).collect(Collectors.toList()));
    }

    private String buildKey(String blobName) {
        assert blobName != null;
        return path + blobName;
    }
}
