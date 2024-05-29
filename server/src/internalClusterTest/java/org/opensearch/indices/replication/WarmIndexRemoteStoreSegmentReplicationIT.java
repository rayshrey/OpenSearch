/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class WarmIndexRemoteStoreSegmentReplicationIT extends SegmentReplicationIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;
    protected boolean clusterSettingsSuppliedByTest = false;

    @Before
    private void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), "partial")
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (clusterSettingsSuppliedByTest) {
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
        } else {
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                //.put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), -1)
                .build();
        }
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITEABLE_REMOTE_INDEX, true);

        return featureSettings.build();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    public void testPrimaryStopped_ReplicaPromoted() throws Exception {
        super.testPrimaryStopped_ReplicaPromoted();
    }

    public void testRestartPrimary() throws Exception {
        super.testRestartPrimary();
    }

    public void testCancelPrimaryAllocation() throws Exception {
        super.testCancelPrimaryAllocation();
    }

    public void testReplicationAfterPrimaryRefreshAndFlush() throws Exception {
        super.testReplicationAfterPrimaryRefreshAndFlush();
    }

    public void testIndexReopenClose() throws Exception {
        super.testIndexReopenClose();
    }

    public void testScrollWithConcurrentIndexAndSearch() throws Exception {
        super.testScrollWithConcurrentIndexAndSearch();
    }

    public void testMultipleShards() throws Exception {
        super.testMultipleShards();
    }

    public void testReplicationAfterForceMerge() throws Exception {
        super.testReplicationAfterForceMerge();
    }

    public void testReplicationAfterForceMergeOnPrimaryShardsOnly() throws Exception {
        super.testReplicationAfterForceMergeOnPrimaryShardsOnly();
    }

    public void testNodeDropWithOngoingReplication() throws Exception {
        super.testNodeDropWithOngoingReplication();
    }

    public void testCancellation() throws Exception {
        super.testCancellation();
    }

    public void testStartReplicaAfterPrimaryIndexesDocs() throws Exception {
        super.testStartReplicaAfterPrimaryIndexesDocs();
    }

    public void testDeleteOperations() throws Exception {
        super.testDeleteOperations();
    }

    public void testReplicationPostDeleteAndForceMerge() throws Exception {
        super.testReplicationPostDeleteAndForceMerge();
    }

    public void testUpdateOperations() throws Exception {
        super.testUpdateOperations();
    }

    public void testReplicaHasDiffFilesThanPrimary() throws Exception {
        super.testReplicaHasDiffFilesThanPrimary();
    }

    public void testDropPrimaryDuringReplication() throws Exception {
        super.testDropPrimaryDuringReplication();
    }

    public void testPressureServiceStats() throws Exception {
        super.testPressureServiceStats();
    }

    public void testScrollCreatedOnReplica() throws Exception {
        assumeFalse(
            "Skipping the test as its not compatible with segment replication with remote store.",
            warmIndexSegmentReplicationEnabled()
        );
        super.testScrollCreatedOnReplica();
    }

    public void testScrollWithOngoingSegmentReplication() {
        assumeFalse(
            "Skipping the test as its not compatible with segment replication with remote store.",
            warmIndexSegmentReplicationEnabled()
        );
    }

    public void testPitCreatedOnReplica() throws Exception {
        assumeFalse(
            "Skipping the test as its not compatible with segment replication with remote store.",
            warmIndexSegmentReplicationEnabled()
        );
        super.testPitCreatedOnReplica();
    }

    public void testPrimaryReceivesDocsDuringReplicaRecovery() throws Exception {
        super.testPrimaryReceivesDocsDuringReplicaRecovery();
    }

    public void testIndexWhileRecoveringReplica() throws Exception {
        super.testIndexWhileRecoveringReplica();
    }

    public void testRestartPrimary_NoReplicas() throws Exception {
        super.testRestartPrimary_NoReplicas();
    }

    public void testRealtimeGetRequestsSuccessful() {
        super.testRealtimeGetRequestsSuccessful();
    }

    public void testRealtimeGetRequestsUnsuccessful() {
        super.testRealtimeGetRequestsUnsuccessful();
    }

    public void testRealtimeMultiGetRequestsSuccessful() {
        super.testRealtimeMultiGetRequestsSuccessful();
    }

    public void testRealtimeMultiGetRequestsUnsuccessful() {
        super.testRealtimeMultiGetRequestsUnsuccessful();
    }

    public void testRealtimeTermVectorRequestsSuccessful() throws IOException {
        super.testRealtimeTermVectorRequestsSuccessful();
    }

    public void testRealtimeTermVectorRequestsUnSuccessful() throws IOException {
        super.testRealtimeTermVectorRequestsUnSuccessful();
    }

    public void testReplicaAlreadyAtCheckpoint() throws Exception {
        super.testReplicaAlreadyAtCheckpoint();
    }

    public void testCancellationDuringGetCheckpointInfo() {
        assumeFalse(
            "Skipping the test as its not compatible with segment replication with remote store.",
            warmIndexSegmentReplicationEnabled()
        );
    }

    public void testCancellationDuringGetSegments() {
        assumeFalse(
            "Skipping the test as its not compatible with segment replication with remote store.",
            warmIndexSegmentReplicationEnabled()
        );
    }

    protected boolean warmIndexSegmentReplicationEnabled() {
        return !Objects.equals(IndexModule.INDEX_STORE_LOCALITY_SETTING.get(indexSettings()).toString(), "partial");
    }

    @After
    public void teardown() {
        clusterSettingsSuppliedByTest = false;
        assertRemoteStoreRepositoryOnAllNodes(REPOSITORY_NAME);
        assertRemoteStoreRepositoryOnAllNodes(REPOSITORY_2_NAME);
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
        clusterAdmin().prepareCleanupRepository(REPOSITORY_2_NAME).get();
    }

    public RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String type = nodeAttributes.get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));

        String settingsAttributeKeyPrefix = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name);
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build());
    }

    public void assertRemoteStoreRepositoryOnAllNodes(String repositoryName) {
        RepositoriesMetadata repositories = internalCluster().getInstance(ClusterService.class, internalCluster().getNodeNames()[0])
            .state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE);
        RepositoryMetadata actualRepository = repositories.repository(repositoryName);

        final RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);

        for (String nodeName : internalCluster().getNodeNames()) {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            DiscoveryNode node = clusterService.localNode();
            RepositoryMetadata expectedRepository = buildRepositoryMetadata(node, repositoryName);

            // Validated that all the restricted settings are entact on all the nodes.
            repository.getRestrictedSystemRepositorySettings()
                .stream()
                .forEach(
                    setting -> assertEquals(
                        String.format(Locale.ROOT, "Restricted Settings mismatch [%s]", setting.getKey()),
                        setting.get(actualRepository.settings()),
                        setting.get(expectedRepository.settings())
                    )
                );
        }
    }

}
