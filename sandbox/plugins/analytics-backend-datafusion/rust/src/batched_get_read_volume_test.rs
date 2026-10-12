/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! How much of a parquet file a batched engine-internal get actually reads.
//!
//! The bulk-update prefetch (`Indexer::prefetchUpdateGets` → `GetService.executeRows`) asks the
//! native side for many rows of one parquet file in a single call, as
//! [`InternalSearch::ByRowIds`]. These tests drive that through the real entry point
//! ([`crate::query_executor::execute_query`]) and measure the volume the engine asks the object
//! store for, by handing it a counting [`ObjectStore`] wrapper — no engine-side counters involved,
//! so the same test runs unchanged against any commit.
//!
//! What they pin down: a get for `n` row positions must read the pages holding those rows, not the
//! whole file. The fixture is sized so the two are far apart — 100 row groups of 1,000 rows with
//! 100-row pages, so a 100-id batch spread across the file touches ~100 of 1,000 pages per column.
//! A predicate-based implementation that cannot prune (DataFusion's statistics rewrite gives up on
//! an `IN` list longer than `MAX_LIST_VALUE_SIZE_REWRITE` = 20, so the pruning predicate collapses
//! to `true`) reads every page of every projected column and filters afterwards; these assertions
//! fail in that case, and the printed summary line carries the measured numbers either way.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::ObjectStoreExt;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::api::DataFusionRuntime;
use crate::datafusion_query_config::{DatafusionQueryConfig, InternalSearch};
use crate::executor::DedicatedExecutor;
use crate::memory::DynamicLimitPool;

// ── Fixture geometry ─────────────────────────────────────────────────────────
//
// 100 row groups x 1_000 rows, 100-row data pages => 10 pages per row group, 1_000 pages per
// column. Chosen so a 100-id batch spread over the file can touch at most ~10% of the pages:
// with far fewer pages than that the two implementations read the same bytes and the test would
// prove nothing.
const NUM_ROWS: i64 = 100_000;
const ROW_GROUP_ROWS: usize = 1_000;
const PAGE_ROWS: usize = 100;

fn fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        // Stored by the writer as the physical row position, 0..N (`merge::schema::append_row_id`,
        // and rewritten to 0..N per chunk on the flush path), which is what makes a row id usable
        // as a row position.
        Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]))
}

/// Writes the fixture file and returns `(temp dir, file size in bytes)`.
fn write_fixture(dir: &std::path::Path) -> u64 {
    let schema = fixture_schema();
    let props = WriterProperties::builder()
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .set_data_page_row_count_limit(PAGE_ROWS)
        // The row-count limit is only checked at write-batch boundaries, so the batch size has to
        // be no larger than the page limit for the pages to come out at PAGE_ROWS.
        .set_write_batch_size(PAGE_ROWS)
        // Dictionary pages are a per-row-group, per-column cost that any reader of that row group
        // pays, whatever rows it wants, so leaving them on would hide the data-page saving behind
        // a dictionary big enough to hold every value in the row group.
        .set_dictionary_enabled(false)
        .build();
    let path = dir.join("fixture.parquet");
    let file = std::fs::File::create(&path).expect("create fixture");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("writer");
    let mut next = 0i64;
    while next < NUM_ROWS {
        let end = (next + PAGE_ROWS as i64).min(NUM_ROWS);
        // Wide enough that the data pages, not the file's page index, dominate its size — the
        // saving under test is in the data pages.
        let ks: Vec<String> = (next..end)
            .map(|i| format!("key-{i:016}-{:-<140}", format!("payload-{i}")))
            .collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(ks)),
            Arc::new(Int64Array::from_iter_values((next..end).map(|i| i * 7))),
            Arc::new(Int64Array::from_iter_values(next..end)),
        ];
        writer
            .write(&RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch"))
            .expect("write");
        next = end;
    }
    writer.close().expect("close");
    std::fs::metadata(&path).expect("stat fixture").len()
}

// ── Counting object store ────────────────────────────────────────────────────

/// Totals of what the engine asked the store for.
#[derive(Debug, Default)]
struct StoreCounters {
    /// Bytes covered by the requested ranges (a whole-object read counts the object's size).
    bytes: AtomicU64,
    /// Ranges requested. A page-granular read asks for many small ranges; a column-chunk read
    /// asks for a few large ones, so this separates "skipped the pages" from "skipped the rows".
    ranges: AtomicU64,
    /// Calls into the store, i.e. round trips rather than ranges.
    calls: AtomicU64,
}

impl StoreCounters {
    fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    fn ranges(&self) -> u64 {
        self.ranges.load(Ordering::Relaxed)
    }
    fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

/// Delegates everything to `inner` and records read volume. Reads are the only thing counted;
/// the write half exists because [`ObjectStore`] requires it.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<StoreCounters>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let requested = options.range.clone();
        let head_only = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head_only {
            let len = match requested {
                Some(range) => range
                    .as_range(result.meta.size)
                    .map(|r| r.end - r.start)
                    .unwrap_or(0),
                None => result.meta.size,
            };
            self.counters.bytes.fetch_add(len, Ordering::Relaxed);
            self.counters.ranges.fetch_add(1, Ordering::Relaxed);
            self.counters.calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.counters.bytes.fetch_add(total, Ordering::Relaxed);
        self.counters
            .ranges
            .fetch_add(ranges.len() as u64, Ordering::Relaxed);
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ── Harness ─────────────────────────────────────────────────────────────────

struct GetOutcome {
    /// `__row_id__` of every row the get returned, ascending. Sorted because arrival order is not
    /// part of the contract — the Java side keys rows by `__row_id__`, and DataFusion is free to
    /// split one file across partitions.
    row_ids: Vec<i64>,
    /// Field names of the returned schema, to compare paths for output parity.
    schema_fields: Vec<String>,
    bytes: u64,
    ranges: u64,
    calls: u64,
}

/// Runs one batched get for `row_ids` against a freshly written fixture, and reports both the rows
/// and the read volume. A fresh fixture (and therefore a cold metadata cache) per call keeps the
/// numbers comparable between cases.
async fn run_batched_get(row_ids: &[i64]) -> (GetOutcome, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    // Canonicalised because LocalFileSystem reports canonical paths while a ListingTableUrl keeps
    // what it was given: on macOS /var is a symlink to /private/var, and the mismatch makes the
    // table's directory listing come back empty.
    let dir_path = std::fs::canonicalize(dir.path()).expect("canonical tempdir");
    let file_size = write_fixture(&dir_path);

    let counters = Arc::new(StoreCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner: Arc::new(LocalFileSystem::new()),
        counters: Arc::clone(&counters),
    });
    let table_path =
        ListingTableUrl::parse(format!("file://{}/", dir_path.display())).expect("table path");
    let location =
        StorePath::from_filesystem_path(dir_path.join("fixture.parquet")).expect("store path");
    let object_meta = store.head(&location).await.expect("head fixture");

    let runtime_env = Arc::new(RuntimeEnvBuilder::new().build().expect("runtime env"));
    let (_pool, dynamic_limit_handle) = DynamicLimitPool::new(1 << 30);
    let runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager: None,
        dynamic_limit_handle,
    };

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();
    let cpu_executor = DedicatedExecutor::new("test-cpu", builder, 2);
    let query_config = DatafusionQueryConfig::test_default();

    // The read volume is what matters here, so the counters are only read after the stream is
    // fully drained — the parquet reader does its IO lazily as batches are pulled.
    let stream_ptr = crate::query_executor::execute_query(
        table_path,
        Arc::new(vec![object_meta]),
        "t".to_string(),
        Vec::new(), // internal search ignores the substrait plan
        &runtime,
        cpu_executor.clone(),
        None,
        &query_config,
        1, // context id
        Arc::clone(&store),
        None,
        &[],
        &[],
        InternalSearch::ByRowIds(Arc::new(row_ids.to_vec())),
    )
    .await
    .expect("batched get");

    let stream = unsafe {
        *Box::from_raw(
            stream_ptr
                as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                    crate::cross_rt_stream::CrossRtStream,
                >,
        )
    };
    let schema_fields = datafusion::physical_plan::RecordBatchStream::schema(&stream)
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let batches = datafusion::physical_plan::common::collect(Box::pin(stream))
        .await
        .expect("collect rows");

    let mut returned = Vec::new();
    for batch in &batches {
        let idx = batch
            .schema()
            .index_of(crate::ROW_ID_COLUMN_NAME)
            .expect("__row_id__ in output");
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("__row_id__ is Int64");
        returned.extend((0..col.len()).map(|i| col.value(i)));
    }

    cpu_executor.join_blocking();
    (
        GetOutcome {
            row_ids: {
                let mut sorted = returned;
                sorted.sort_unstable();
                sorted
            },
            schema_fields,
            bytes: counters.bytes(),
            ranges: counters.ranges(),
            calls: counters.calls(),
        },
        file_size,
    )
}

fn report(case: &str, outcome: &GetOutcome, file_size: u64) {
    println!(
        "BATCH-GET {case}: rows={} store_bytes={} ({:.1}% of {} byte file) ranges={} calls={}",
        outcome.row_ids.len(),
        outcome.bytes,
        100.0 * outcome.bytes as f64 / file_size as f64,
        file_size,
        outcome.ranges,
        outcome.calls,
    );
}

/// Read volume must track the pages holding the ids, not the file. The bound is deliberately loose
/// (a quarter of the file for ~10% of the pages) so page layout or footer size can drift without
/// the test turning flaky; a whole-file read lands at ~100% and fails by a wide margin.
#[tokio::test]
async fn batched_get_of_spread_ids_reads_a_fraction_of_the_file() {
    let _globals = crate::test_process_globals::lock();
    // 100 ids, one per row group, so no row group can be skipped and the saving has to come from
    // page selection within each one.
    let ids: Vec<i64> = (0..100).map(|i| i * 1_000 + 7).collect();

    let (outcome, file_size) = run_batched_get(&ids).await;
    report("100 spread ids", &outcome, file_size);

    assert_eq!(outcome.row_ids, ids, "a get must return exactly its ids");
    assert!(
        outcome.bytes < file_size / 4,
        "a 100-id get read {} of {} bytes ({:.1}%) — the scan is not restricted to the pages \
         holding those rows",
        outcome.bytes,
        file_size,
        100.0 * outcome.bytes as f64 / file_size as f64,
    );
}

/// Ids packed into one page are the cheapest case: one page per column, and 99 of 100 row groups
/// untouched.
#[tokio::test]
async fn batched_get_of_clustered_ids_reads_one_page_per_column() {
    let _globals = crate::test_process_globals::lock();
    let ids: Vec<i64> = (50_000..50_050).collect();

    let (outcome, file_size) = run_batched_get(&ids).await;
    report("50 clustered ids", &outcome, file_size);

    assert_eq!(outcome.row_ids, ids, "a get must return exactly its ids");
    assert!(
        outcome.bytes < file_size / 10,
        "a 50-id single-page get read {} of {} bytes ({:.1}%)",
        outcome.bytes,
        file_size,
        100.0 * outcome.bytes as f64 / file_size as f64,
    );
}

/// 21 ids is the first batch size DataFusion's statistics rewrite refuses to expand
/// (`MAX_LIST_VALUE_SIZE_REWRITE` = 20), so a predicate-based get stops pruning here while a
/// positional read does not care.
#[tokio::test]
async fn batched_get_just_past_the_in_list_rewrite_limit_still_prunes() {
    let _globals = crate::test_process_globals::lock();
    let ids: Vec<i64> = (0..21).map(|i| i * 4_000 + 3).collect();

    let (outcome, file_size) = run_batched_get(&ids).await;
    report("21 spread ids", &outcome, file_size);

    assert_eq!(outcome.row_ids, ids, "a get must return exactly its ids");
    assert!(
        outcome.bytes < file_size / 7,
        "a 21-id get read {} of {} bytes ({:.1}%)",
        outcome.bytes,
        file_size,
        100.0 * outcome.bytes as f64 / file_size as f64,
    );
}

/// Ids are single rows, row-group-boundary rows, and the file's first and last row; the batch must
/// come back whole and in ascending order whatever the geometry.
#[tokio::test]
async fn batched_get_returns_boundary_rows() {
    let _globals = crate::test_process_globals::lock();
    let ids: Vec<i64> = vec![
        0,
        1,
        (PAGE_ROWS - 1) as i64,
        PAGE_ROWS as i64,
        (ROW_GROUP_ROWS - 1) as i64,
        ROW_GROUP_ROWS as i64,
        NUM_ROWS / 2,
        NUM_ROWS - 1,
    ];

    let (outcome, file_size) = run_batched_get(&ids).await;
    report("boundary ids", &outcome, file_size);
    assert_eq!(outcome.row_ids, ids);
}

/// A single id is the shape the old per-document get had, and must stay the cheapest read.
#[tokio::test]
async fn batched_get_of_one_id_reads_one_row() {
    let _globals = crate::test_process_globals::lock();
    let ids = vec![NUM_ROWS - 7];

    let (outcome, file_size) = run_batched_get(&ids).await;
    report("1 id", &outcome, file_size);

    assert_eq!(outcome.row_ids, ids);
    assert!(
        outcome.bytes < file_size / 10,
        "a 1-id get read {} of {} bytes",
        outcome.bytes,
        file_size
    );
}

/// Row ids past the end of the file cannot be served. They must drop out of the result — the
/// caller treats a missing id as "read it individually" — rather than fail the whole batch.
#[tokio::test]
async fn batched_get_skips_ids_beyond_the_file() {
    let _globals = crate::test_process_globals::lock();
    let ids = vec![10, NUM_ROWS, NUM_ROWS + 5_000, NUM_ROWS - 1];

    let (outcome, _file_size) = run_batched_get(&ids).await;
    assert_eq!(outcome.row_ids, vec![10, NUM_ROWS - 1]);
}

/// Every column of the row is returned, in the file's field order: the Java side maps the batch
/// back to a document by field name, so a get that quietly dropped or reordered columns would
/// corrupt the update it feeds.
#[tokio::test]
async fn batched_get_returns_every_column() {
    let _globals = crate::test_process_globals::lock();
    let (outcome, _file_size) = run_batched_get(&[1, 2, 3]).await;
    assert_eq!(
        outcome.schema_fields,
        vec![
            "k".to_string(),
            "v".to_string(),
            crate::ROW_ID_COLUMN_NAME.to_string()
        ]
    );
}
