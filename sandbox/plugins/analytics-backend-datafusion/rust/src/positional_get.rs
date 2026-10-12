/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Reads a known set of row positions out of one parquet file.
//!
//! This serves the batched engine-internal get: the bulk-update prefetch
//! ([`Indexer::prefetchUpdateGets`] → `GetService.executeRows`) resolves every update's id through
//! the Lucene secondary index into `(writer generation, row id)`, groups them by file, and asks for
//! one file's rows in a single call ([`InternalSearch::ByRowIds`]).
//!
//! `__row_id__` is the physical row position the writer stamps at flush (sequential `0..N` per
//! file, rewritten per chunk after any index sort, and restamped from 0 by a merge), so a row id
//! *is* a row position and the rows wanted are known before the read starts. That makes a predicate
//! the wrong tool: an `IN` list over `__row_id__` only prunes while DataFusion's statistics rewrite
//! will expand it, which it refuses to do past `MAX_LIST_VALUE_SIZE_REWRITE` (20) entries
//! (`datafusion_pruning::pruning_predicate`). Past that the pruning predicate collapses to `true`,
//! so no row group and no page is skipped: the scan decodes every page of every projected column
//! and drops all but the handful of matching rows, which costs more than the per-document gets the
//! batch replaced.
//!
//! Instead the positions go straight to parquet as a [`RowSelection`] per row group inside a
//! [`ParquetAccessPlan`], with no predicate at all. Row groups holding none of the ids are marked
//! `RowGroupAccess::None` and never opened; inside the others, arrow-rs intersects the selection
//! with the page offset index and fetches only the pages that overlap a selected row
//! (`RowSelection::scan_ranges`), decompressing each such page once however many ids it holds.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};
use roaring::RoaringBitmap;

use crate::api::DataFusionRuntime;
use crate::indexed_table::parquet_bridge::{
    create_exec_with_access_plan, load_parquet_metadata_with_meta, ReadIoStats,
    RowGroupStreamConfig,
};
use crate::indexed_table::row_selection::build_row_selection_with_min_skip_run;
use crate::query_executor::store_url_from_table_path;

/// Reads the rows at `row_ids` from the single parquet file in `object_metas`, in file order.
///
/// Returns the stream and its plan. Every column of the file is projected, including
/// `__row_id__`: the caller matches rows back to documents by the row id in the row itself.
///
/// Row ids outside the file are dropped rather than failing the batch — the prefetch is
/// best-effort by contract, and an id missing from the result is read individually on the normal
/// per-document path. An empty `row_ids` is a caller bug and is rejected.
pub async fn read_rows_at_positions(
    table_path: &ListingTableUrl,
    object_metas: &[ObjectMeta],
    store: Arc<dyn ObjectStore>,
    runtime: &DataFusionRuntime,
    row_ids: &[i64],
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>), DataFusionError> {
    if row_ids.is_empty() {
        return Err(DataFusionError::Execution(
            "internal search ByRowIds: empty row-id list".to_string(),
        ));
    }
    // One generation's file per call: `GetService.executeRows` resolves the ids to a single
    // `WriterFileSet` and opens a reader over just that file, so the ids are positions in it.
    let [object_meta] = object_metas else {
        return Err(DataFusionError::Execution(format!(
            "internal search ByRowIds expects exactly one parquet file, got {}",
            object_metas.len()
        )));
    };

    // Footer-only metadata, shared with every other reader of this file through the global
    // file-metadata cache.
    let (file_schema, file_size, footer) = load_parquet_metadata_with_meta(
        Arc::clone(&store),
        &object_meta.location,
        object_meta.clone(),
        runtime.runtime_env.cache_manager.get_file_metadata_cache(),
    )
    .await
    .map_err(DataFusionError::Execution)?;

    let access_plan = build_access_plan(&footer, row_ids);

    // Give the reader an offset index so the selection can be resolved to page byte ranges;
    // without it arrow-rs still skips the pages' *decompression* but has to fetch each touched
    // column chunk whole. Scoped to the columns this scan reads — which is all of them, since a
    // get returns the whole row — and cached process-wide, so repeat gets against the same file
    // pay for it once. Only the OffsetIndex is requested (no predicate columns, so no
    // ColumnIndex: there is no predicate to prune with). A `None` result means the index could
    // not be loaded; the footer still reads correctly, just with whole-chunk fetches.
    let read_columns: Vec<usize> =
        (0..footer.file_metadata().schema_descr().num_columns()).collect();
    let metadata = crate::cache::page_index::load_scoped_page_index_cols(
        &store,
        &object_meta.location,
        &footer,
        &[],
        &read_columns,
    )
    .await
    .unwrap_or_else(|| Arc::clone(&footer));

    // Match the output types the ListingTable path produces for the per-document get: with
    // DataFusion's `schema_force_view_types` (on by default) a parquet read of a string column
    // yields Utf8View, and the Java side decodes whatever the single-row get produced. Declaring
    // the view schema here makes the opener coerce during decode, exactly as the table path does.
    let logical_schema: SchemaRef = if datafusion::config::ConfigOptions::default()
        .execution
        .parquet
        .schema_force_view_types
    {
        Arc::new(crate::schema_coerce::transform_schema_to_view_recursive(
            &file_schema,
        ))
    } else {
        file_schema
    };

    let config = RowGroupStreamConfig {
        file_path: object_meta.location.to_string(),
        file_size,
        store,
        store_url: store_url_from_table_path(table_path)?,
        full_schema: logical_schema,
        metadata,
        // Whole row: the get feeds a document update, which needs every field.
        projection: None,
        // Positions already say which rows to read; a predicate would only re-derive them.
        predicate: None,
        io_stats: Arc::new(ReadIoStats::default()),
    };
    let exec = create_exec_with_access_plan(&config, access_plan, false)?;

    // Same output-schema narrowing the ListingTable path applies, so the batches Java decodes are
    // identical to those the per-document get produced.
    let target_schema = crate::schema_coerce::coerce_inferred_schema(exec.schema());
    let exec = crate::relabel_exec::wrap_if_relabel_needed(exec, target_schema)?;

    let stream = exec.execute(0, Arc::new(datafusion::execution::TaskContext::default()))?;
    Ok((stream, exec))
}

/// Maps row positions to a per-row-group [`RowSelection`], leaving row groups that hold none of
/// them unread.
fn build_access_plan(
    footer: &Arc<parquet::file::metadata::ParquetMetaData>,
    row_ids: &[i64],
) -> ParquetAccessPlan {
    let num_row_groups = footer.num_row_groups();
    let mut row_group_rows: Vec<u64> = Vec::with_capacity(num_row_groups);
    // First row position of each row group, so a position can be resolved to (row group, offset).
    let mut first_rows: Vec<u64> = Vec::with_capacity(num_row_groups);
    let mut total: u64 = 0;
    for i in 0..num_row_groups {
        let rows = footer.row_group(i).num_rows().max(0) as u64;
        first_rows.push(total);
        row_group_rows.push(rows);
        total += rows;
    }

    let mut per_row_group: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    for &id in row_ids {
        if id < 0 || id as u64 >= total {
            continue; // not in this file — the caller re-reads it individually
        }
        let position = id as u64;
        // `first_rows` is ascending, so the owning row group is the last one starting at or before
        // this position.
        let row_group = first_rows.partition_point(|&start| start <= position) - 1;
        let offset = (position - first_rows[row_group]) as u32;
        per_row_group.entry(row_group).or_default().insert(offset);
    }

    let mut access_plan = ParquetAccessPlan::new_none(num_row_groups);
    for (row_group, offsets) in per_row_group {
        // `min_skip_run = 1`: skip every unselected row. A get asks for individual rows, so
        // coarsening the selection to keep the selector list short would only read rows the
        // caller did not ask for.
        let selection =
            build_row_selection_with_min_skip_run(&offsets, row_group_rows[row_group] as usize, 1);
        access_plan.set(row_group, RowGroupAccess::Selection(selection));
    }
    access_plan
}
