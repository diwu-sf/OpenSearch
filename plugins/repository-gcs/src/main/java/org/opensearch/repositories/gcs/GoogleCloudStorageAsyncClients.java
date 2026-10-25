/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.repositories.gcs.async.AsyncExecutorContainer;

/** Priority-specific async clients sharing the repository's existing cached Storage instance. */
record GoogleCloudStorageAsyncClients(AsyncExecutorContainer client, AsyncExecutorContainer priorityClient,
    AsyncExecutorContainer urgentClient) {
}
