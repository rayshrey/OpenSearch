/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@ExperimentalApi
public class IndexFileDeleter {
    
    private final Map<String, AtomicInteger> fileRefCounts = new ConcurrentHashMap<>();
    private final CompositeIndexingExecutionEngine engine;
    private static final Pattern FILE_PATTERN = Pattern.compile(".*_(\\d+)\\.parquet$", Pattern.CASE_INSENSITIVE);
    private final ShardPath shardPath;
    
    public IndexFileDeleter(CompositeIndexingExecutionEngine engine, CatalogSnapshot initialCatalogSnapshot, ShardPath shardPath) throws IOException {
        this.engine = engine;
        this.shardPath = shardPath;
        if (initialCatalogSnapshot != null) {
            addFileReferences(initialCatalogSnapshot);
            trimShardPath(shardPath);
        }
    }

    public void addFileReferences(CatalogSnapshot snapshot) {
        Set<String> filePaths = extractFilePaths(snapshot);
        for (String filePath : filePaths) {
            fileRefCounts.computeIfAbsent(filePath, k -> new AtomicInteger(0)).incrementAndGet();
        }
        System.out.println("IndexFileDeleter after addFileReferences: " + this.toString());
    }
    
    public void removeFileReferences(CatalogSnapshot snapshot) {
        Set<String> filePaths = extractFilePaths(snapshot);
        Set<String> filesToDelete = new HashSet<>();
        
        for (String filePath : filePaths) {
            AtomicInteger refCount = fileRefCounts.get(filePath);
            if (refCount != null && refCount.decrementAndGet() == 0) {
                fileRefCounts.remove(filePath);
                filesToDelete.add(shardPath.getDataPath().resolve(filePath).toString());
            }
        }
        
        if (!filesToDelete.isEmpty()) {
            System.out.println("Files to delete : " + filesToDelete);
            deleteUnreferencedFiles(filesToDelete);
        }
        System.out.println("IndexFileDeleter after removeFileReferences: " + this.toString());
    }
    
    private Set<String> extractFilePaths(CatalogSnapshot snapshot) {
        Set<String> filePaths = new HashSet<>();
        
        for (String dataFormat : snapshot.dfGroupedSearchableFiles.keySet()) {
            Collection<WriterFileSet> fileSets = snapshot.getSearchableFiles(dataFormat);
            for (WriterFileSet fileSet : fileSets) {
                filePaths.addAll(fileSet.getFiles());
            }
        }
        
        return filePaths;
    }

    private void trimShardPath(ShardPath shardPath) throws IOException {
        Set<String> referencedFiles = new HashSet<>(fileRefCounts.keySet());
        Set<String> filesToDelete = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardPath.getDataPath(), "*.parquet")) {
            StreamSupport.stream(stream.spliterator(), false)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter((entry) -> (!referencedFiles.contains(entry)))
                    .forEach(filesToDelete::add);
        }
        filesToDelete = filesToDelete.stream().map(entry -> shardPath.getDataPath().resolve(entry).toString()).collect(Collectors.toSet());
        System.out.println("IndexFileDeleter filesToDelete (on initialization) : " + filesToDelete);

        if (!filesToDelete.isEmpty()) {
            System.out.println("Files to delete : " + filesToDelete);
            deleteUnreferencedFiles(filesToDelete);
        }
    }
    
    private void deleteUnreferencedFiles(Set<String> filesToDelete) {
        try {
            engine.deleteFiles(filesToDelete);
        } catch (Exception e) {
            System.err.println("Failed to delete unreferenced files: " + filesToDelete + ", error: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexFileDeleter{fileRefCounts={");
        boolean first = true;
        for (Map.Entry<String, AtomicInteger> entry : fileRefCounts.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue().get());
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }
}