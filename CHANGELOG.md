# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.4.4] - [8 October 2015]

## Enhanced
- Upgraded mesos dependancy to 0.24.1

## [0.4.3] - [28 September 2015]

## Enhanced
- [Include seed data in system-test:main](https://github.com/mesos/elasticsearch/issues/312)
- [Documentation migration](https://github.com/mesos/elasticsearch/pull/337)

## [0.4.2] - [24 September 2015]

### Fixed
- [Executor does not call driver.stop() when killed bug](https://github.com/mesos/elasticsearch/issues/293)
- [Healthcheck thread not stopped during Unit tests bug testing](https://github.com/mesos/elasticsearch/issues/323)

- [Reimplement scheduler unit tests refactoring](https://github.com/mesos/elasticsearch/issues/321)
- [Executor timeout large value bug](https://github.com/mesos/elasticsearch/issues/318)
- [When executors are lost, the status is not propagated back to zookeeper bug](https://github.com/mesos/elasticsearch/issues/310)
- [Executor Timeout reset and timeout units bug](https://github.com/mesos/elasticsearch/issues/308)

## Enhanced
- [Update DCOS to 0.4.1 enhancement](https://github.com/mesos/elasticsearch/issues/305)
- [Test coverage for healthcheck mechanism bug](https://github.com/mesos/elasticsearch/issues/303)

## [0.4.1] - [15 September 2015]

### Fixed
- [GUI correctly represents state of cluster](https://github.com/mesos/elasticsearch/issues/206)
- [Request resources as role](https://github.com/mesos/elasticsearch/issues/284)
- [Zookeeper unable to create /es node - Incorrect zk formatter](https://github.com/mesos/elasticsearch/issues/286)

### Added
- [GUI updates - Charting, placeholder for scaling](https://github.com/mesos/elasticsearch/pull/263)

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