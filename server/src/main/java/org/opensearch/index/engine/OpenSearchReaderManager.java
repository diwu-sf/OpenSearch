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

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

import java.io.IOException;
import java.util.function.LongSupplier;

/**
 * Utility class to safely share {@link OpenSearchDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @see SearcherManager
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "reference counting is required here")
class OpenSearchReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {
    private final LongSupplier lastCommitGeneration;
    private final CheckedSupplier<OpenSearchDirectoryReader, IOException> openReader;

    /**
     * Creates and returns a new OpenSearchReaderManager from the given
     * already-opened {@link OpenSearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     */
    OpenSearchReaderManager(OpenSearchDirectoryReader reader) {
        this(reader, () -> -1, null);
    }

    /**
     * Creates a manager for near-real-time readers of an index writer, which also reopens a reader that names an older commit
     * than the writer's latest one.
     *
     * @param reader               the directoryReader to use for future reopens
     * @param lastCommitGeneration the generation of the index writer's latest commit
     * @param openReader           opens a new near-real-time reader on the index writer
     */
    OpenSearchReaderManager(
        OpenSearchDirectoryReader reader,
        LongSupplier lastCommitGeneration,
        CheckedSupplier<OpenSearchDirectoryReader, IOException> openReader
    ) {
        this.current = reader;
        this.lastCommitGeneration = lastCommitGeneration;
        this.openReader = openReader;
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        final OpenSearchDirectoryReader reader = (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        if (reader == null && namesOlderCommit(referenceToRefresh)) {
            // IndexWriter#commit advances the segments version before it syncs the commit's files, and the generation only
            // after. A reader opened in between has the new version with the previous generation, so Lucene considers it
            // current, yet it names a segments_N file that the commit replaces and may delete. Open a reader on the new commit.
            return openReader.get();
        }
        return reader;
    }

    private boolean namesOlderCommit(OpenSearchDirectoryReader reader) {
        return openReader != null
            && FilterDirectoryReader.unwrap(reader) instanceof StandardDirectoryReader standardReader
            && standardReader.getSegmentInfos().getLastGeneration() < lastCommitGeneration.getAsLong();
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}
