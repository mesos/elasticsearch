# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.4.0] - [7 September 2015]

### Added

- [Configurable data directory](https://github.com/mesos/elasticsearch/issues/275)
- [Support framework roles](https://github.com/mesos/elasticsearch/pull/281)

###

- Upgraded to [Mini-Mesos 0.2.6](https://github.com/ContainerSolutions/mini-mesos/releases/tag/0.2.6) with fast-fail logging

## [0.3.0] - [4 September 2015]
### Added
- [Full configuration of framework via CLI](https://github.com/mesos/elasticsearch/issues/111)
- [User can override ES settings with their own elasticsearch.yml](https://github.com/mesos/elasticsearch/issues/242)
- [Data is now stored in Docker volumes](https://github.com/mesos/elasticsearch/issues/188)
- [Support multiple Elasticsearch clusters on single Mesos cluster](https://github.com/mesos/elasticsearch/issues/91)
- [Configurable scheduler and executor images](https://github.com/mesos/elasticsearch/issues/71)
- [Support separate Zookeeper cluster for the framework](https://github.com/mesos/elasticsearch/pull/245)
- [Fixed UI redirection bug](https://github.com/mesos/elasticsearch/pull/239)

### Changed
- Removed zk, m, n and ram parameters from old CLI
- CLI code now uses JCommander library