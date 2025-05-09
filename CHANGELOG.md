# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
<<<<<<< HEAD
- Renaming the node role search to warm ([#17573](https://github.com/opensearch-project/OpenSearch/pull/17573))
=======
- [Rule based auto-tagging] Add get rule API ([#17336](https://github.com/opensearch-project/OpenSearch/pull/17336))
- Add multi-threaded writer support in pull-based ingestion ([#17912](https://github.com/opensearch-project/OpenSearch/pull/17912))
- Unset discovery nodes for every transport node actions request ([#17682](https://github.com/opensearch-project/OpenSearch/pull/17682))
- Implement parallel shard refresh behind cluster settings ([#17782](https://github.com/opensearch-project/OpenSearch/pull/17782))
- Bump OpenSearch Core main branch to 3.0.0 ([#18039](https://github.com/opensearch-project/OpenSearch/pull/18039))
- Update API of Message in index to add the timestamp for lag calculation in ingestion polling ([#17977](https://github.com/opensearch-project/OpenSearch/pull/17977/))
<<<<<<< HEAD
- Enabled default throttling for all tasks submitted to cluster manager ([#17711](https://github.com/opensearch-project/OpenSearch/pull/17711))
=======
- Add Warm Disk Threshold Allocation Decider for Warm shards ([#18082](https://github.com/opensearch-project/OpenSearch/pull/18082))
>>>>>>> c677397504c (Add Warm Disk Threshold Allocation Decider for Warm shards (#18082))
- Add composite directory factory ([#17988](https://github.com/opensearch-project/OpenSearch/pull/17988))

### Changed
- Change the default max header size from 8KB to 16KB. ([#18024](https://github.com/opensearch-project/OpenSearch/pull/18024))
- Avoid invalid retries in multiple replicas when querying [#17370](https://github.com/opensearch-project/OpenSearch/pull/17370)
* Enable concurrent_segment_search auto mode by default[#17978](https://github.com/opensearch-project/OpenSearch/pull/17978)
- Skip approximation when `track_total_hits` is set to `true` [#18017](https://github.com/opensearch-project/OpenSearch/pull/18017)

>>>>>>> d744b40ef78 (Add composite directory factory (#17988))
### Dependencies
- Bump `netty` from 4.1.118.Final to 4.1.121.Final ([#18192](https://github.com/opensearch-project/OpenSearch/pull/18192))

### Deprecated

### Removed

### Fixed
- Use Bad Request status for InputCoercionException ([#18161](https://github.com/opensearch-project/OpenSearch/pull/18161))
- Avoid NPE if on SnapshotInfo if 'shallow' boolean not present ([#18187](https://github.com/opensearch-project/OpenSearch/issues/18187))
- Null check field names in QueryStringQueryBuilder ([#18194](https://github.com/opensearch-project/OpenSearch/pull/18194))
- Fix illegal argument exception when creating a PIT ([#16781](https://github.com/opensearch-project/OpenSearch/pull/16781))

### Security

### Changed

- Change single shard assignment log message from warn to debug ([#18186](https://github.com/opensearch-project/OpenSearch/pull/18186))

[Unreleased 2.19.x]: https://github.com/opensearch-project/OpenSearch/compare/fd9a9d90df25bea1af2c6a85039692e815b894f5...2.19
