# Asynchronous GCS transfers

The GCS blob container implements `AsyncMultiStreamBlobContainer`. Metadata,
remote segment and translog uploads use the existing OpenSearch async callers.
GCS SDK I/O runs on node-managed virtual threads.

Each repository retains its existing cached `Storage` instance. Repositories
using the same named GCS client share three logical async clients: URGENT, HIGH,
and NORMAL/LOW. Each group has an independent executor and operation-admission
budget. Reads and deletes use the NORMAL/LOW client. These budgets isolate
admission; they do not create separate HTTP connection pools or reserve network
bandwidth.

## Client limits

Configure these settings under `gcs.client.<name>.` in `opensearch.yml`:

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_concurrent_operations` | `500` | Active operations in each priority group |
| `max_pending_operations` | `10000` | Additional admitted operations waiting to execute |
| `operation_acquisition_timeout` | `15m` | Maximum wait for an active-operation permit |

There is no fixed node-wide limit of 100. Each priority group has its own budget,
and each named client has its own set of groups. A returned read stream retains
its active and admission permits until closed. Callers must close streams even
when reading fails or is abandoned.

## Transfer permits and queues

NORMAL and LOW uploads additionally share node-wide transfer permits, following
the S3 plugin's allocation and borrowing policy:

- Total permits: `max(4 * allocatedProcessors, 10)`.
- NORMAL receives 70% by default, rounded down; LOW receives the remainder.
- NORMAL may borrow LOW permits when the LOW allocation was idle when the
  transfer's request context was created.
- LOW may borrow NORMAL permits while more than 40% of the NORMAL allocation
  remains available.
- HIGH and URGENT uploads bypass transfer permits and transfer queues. They still
  acquire their own client's operation permits.

| Node setting | Default |
| --- | --- |
| `gcs_priority_permit_alloc_perc` | `70` |
| `gcs_permit_wait_duration_min` | `5` |
| `gcs_transfer_queue_consumers` | `max(5, 2 * allocatedProcessors)` |

NORMAL and LOW have separate transfer queues. As in S3, their byte budgets are
10 GiB per NORMAL consumer and 20 GiB per LOW consumer. LOW consumer count is
`max(2, floor((100 - allocationPercent) / 100 * normalConsumers))`. Each queue
also admits at most 10,000 queued items. The byte budget measures the sizes of
queued transfers, not bytes buffered in memory. An oversized file occupies a
whole queue budget and still uploads asynchronously. A full or closed queue
fails the upload through its completion listener.

## Upload lifecycle

`GoogleCloudStorageBlobContainer` chooses the priority client.
`AsyncTransferManager` routes NORMAL/LOW uploads through `SizeBasedBlockingQ`,
acquires a `TransferSemaphoresHolder` permit, and submits work to the selected
`AsyncExecutorContainer`. Permit waits happen on virtual threads.
`AsyncPartsHandler` assembles the supplied streams and performs the SDK upload.

A transfer permit covers an entire GCS resumable upload, including session
restarts. GCS chunks remain sequential within one upload; independent objects
upload concurrently. This differs from S3's independent multipart requests,
where a permit can cover an individual part. The GCS client-operation permit
also covers the complete upload, so it is coarser than an HTTP connection limit.

GCS uses CRC32C, while OpenSearch's upload finalizer validates CRC32. The plugin
therefore requests local integrity checking and invokes the finalizer before
creating or committing an object. Failed resumable sessions remain uncommitted
and expire. Async reads pin the object generation so a retry cannot silently
read a replacement object.
