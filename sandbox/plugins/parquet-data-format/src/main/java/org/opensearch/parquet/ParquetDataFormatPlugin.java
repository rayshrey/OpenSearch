/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.engine.ParquetIndexingEngine;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * OpenSearch plugin providing the Parquet data format for indexing operations.
 *
 * <p>Implements {@link DataFormatPlugin} to register the Parquet format with OpenSearch's
 * data format framework. On node startup, captures cluster settings via
 * {@link #createComponents} and passes them to the per-shard
 * {@link ParquetIndexingEngine} instances created in {@link #indexingEngine}.
 *
 * <p>The descriptor provides a {@link PrecomputedChecksumStrategy} that the directory
 * holds at construction time. The {@link ParquetIndexingEngine} receives the same
 * strategy instance from the directory via
 * {@link org.opensearch.index.store.DataFormatAwareStoreDirectory#getChecksumStrategy},
 * so pre-computed CRC32 values registered during write are directly visible to the
 * upload path — no post-construction wiring needed.
 *
 * <p>Registers plugin settings defined in {@link ParquetSettings}.
 */
public class ParquetDataFormatPlugin extends Plugin implements DataFormatPlugin {

    /** Thread pool name for background native Parquet writes during VSR rotation. */
    public static final String PARQUET_THREAD_POOL_NAME = "parquet_native_write";

    private static final ParquetDataFormat dataFormat = new ParquetDataFormat();
    /** Initialized to EMPTY to avoid NPE if indexingEngine() is called before createComponents(). */
    private Settings settings = Settings.EMPTY;
    private ThreadPool threadPool;

    /** Creates a new ParquetDataFormatPlugin. */
    public ParquetDataFormatPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.settings = clusterService.getSettings();
        this.threadPool = threadPool;
        return Collections.emptyList();
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public NativeStoreHandle createNativeStore(Store store) {
        // Phase 2 implementation outline:
        //
        // 1. Get the repository-level native store from the Store
        //    NativeStoreRepository repoStore = store.getNativeStoreRepository();
        //
        // 2. If a repository-level native store exists (remote-backed index),
        //    create a PrefixObjectStore scoped to this shard's path within the repository.
        //    This ensures the shard can only access its own files.
        //    if (repoStore.isLive()) {
        //        String shardPrefix = "indices/" + indexUUID + "/" + shardId + "/parquet/";
        //        long scopedPtr = RustBridge.createScopedStore(repoStore.getPointer(), shardPrefix);
        //        return new NativeStoreHandle(scopedPtr, RustBridge::destroyStore);
        //    }
        //
        // 3. Otherwise (local-only index), create a LocalFileSystem ObjectStore
        //    rooted at the shard's data path.
        //    long localPtr = RustBridge.createLocalStore(store.shardPath().getDataPath().toString());
        //    if (localPtr <= 0) return NativeStoreHandle.EMPTY;
        //    return new NativeStoreHandle(localPtr, RustBridge::destroyStore);
        //
        // TODO: Implement RustBridge.createLocalStore(String rootPath) — FFM call to
        //       parquet_create_local_store which creates LocalFileSystem::new_with_prefix(root)
        // TODO: Implement RustBridge.createScopedStore(long parentHandle, String prefix) — FFM call to
        //       parquet_create_scoped_store which creates PrefixStore wrapping the parent store
        // TODO: Implement RustBridge.destroyStore(long handle) — FFM call to
        //       parquet_destroy_store which drops the Arc<dyn ObjectStore>
        // TODO: Implement Rust object_store_handle.rs with create_local_store, create_scoped_store, drop_store
        // TODO: Add FFM exports in ffm.rs for the above functions

        return NativeStoreHandle.EMPTY;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig engineConfig, FormatChecksumStrategy checksumStrategy) {
        return new ParquetIndexingEngine(
            settings,
            dataFormat,
            engineConfig.store().shardPath(),
            () -> ArrowSchemaBuilder.getSchema(engineConfig.mapperService()),
            engineConfig.indexSettings(),
            threadPool,
            checksumStrategy
        );
    }

    @Override
    public Map<String, DataFormatDescriptor> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        return Map.of(
            ParquetDataFormat.PARQUET_DATA_FORMAT_NAME,
            new DataFormatDescriptor(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME, new PrecomputedChecksumStrategy())
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return ParquetSettings.getSettings();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                PARQUET_THREAD_POOL_NAME,
                OpenSearchExecutors.allocatedProcessors(settings),
                -1,
                "thread_pool." + PARQUET_THREAD_POOL_NAME
            )
        );
    }
}
