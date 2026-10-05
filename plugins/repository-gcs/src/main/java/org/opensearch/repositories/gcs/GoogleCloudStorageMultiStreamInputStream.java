/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.common.StreamContext;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** Streams upload parts in order, reopening them when a resumable upload must restart. */
class GoogleCloudStorageMultiStreamInputStream extends InputStream {
    private final StreamContext context;
    private final long length;
    private InputStream current;
    private int nextPart;
    private long position;
    private boolean closed;

    GoogleCloudStorageMultiStreamInputStream(StreamContext context, long length) {
        this.context = context;
        this.length = length;
    }

    @Override
    public int read() throws IOException {
        byte[] single = new byte[1];
        return read(single, 0, 1) == -1 ? -1 : single[0] & 0xff;
    }

    @Override
    public int read(byte[] bytes, int offset, int count) throws IOException {
        Objects.checkFromIndexSize(offset, count, bytes.length);
        if (closed) {
            throw new IOException("Upload stream is closed");
        }
        if (count == 0) {
            return 0;
        }
        while (true) {
            if (current == null) {
                if (nextPart == context.getNumberOfParts()) {
                    if (position != length) {
                        throw new EOFException("Expected " + length + " upload bytes but read " + position);
                    }
                    return -1;
                }
                current = context.provideStream(nextPart++).getInputStream();
            }
            int read = current.read(bytes, offset, count);
            if (read >= 0) {
                position += read;
                if (position > length) {
                    throw new IOException("Upload stream exceeds expected length " + length);
                }
                return read;
            }
            current.close();
            current = null;
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        if (position != 0) {
            throw new IllegalStateException("Only a mark at the start of an upload is supported");
        }
    }

    @Override
    public void reset() throws IOException {
        if (closed) {
            throw new IOException("Upload stream is closed");
        }
        if (current != null) {
            current.close();
            current = null;
        }
        nextPart = 0;
        position = 0;
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            if (current != null) {
                current.close();
            }
        }
    }
}
