# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.7.1] - [25 January 2016]

### Bugs

- [Always load default yml file first then load custom yml](https://github.com/mesos/elasticsearch/issues/465)
- [Load yml from URL](https://github.com/mesos/elasticsearch/issues/464)
- [Documentation](https://github.com/mesos/elasticsearch/issues/432)

## [0.7.0] - [15 January 2016]

### Features
- [Update to minimesos 0.5.0+](https://github.com/mesos/elasticsearch/issues/421)
- [Upgrade to elasticsearch 2.1.1](https://github.com/mesos/elasticsearch/issues/415)
- [Allow a user to specify his own http/transport ports](https://github.com/mesos/elasticsearch/issues/413)
- [Manual horizontal scaling](https://github.com/mesos/elasticsearch/issues/403)
- Networking overhaul. Now uses standard ES zen for clustering.

### Bugs

- [shouldNotLoseDataWhenScalingDown system test occasionally fails](https://github.com/mesos/elasticsearch/issues/447)
- [Minimesos hostnames do not point to running containers](https://github.com/mesos/elasticsearch/issues/445)
- [Fix Jenkins system test failures](https://github.com/mesos/elasticsearch/issues/436)
- [Option to use IP addresses instead of hostname](https://github.com/mesos/elasticsearch/issues/418)
- [Documentation](https://github.com/mesos/elasticsearch/issues/417)
- [Jitpack cannot build jar beacuse of docker build](https://github.com/mesos/elasticsearch/issues/408)

### Refactoring

- [Remove dependency on ES Zookeeper](https://github.com/mesos/elasticsearch/issues/439)
- [Sanity check for framework registration before getting cluster state](https://github.com/mesos/elasticsearch/issues/410)

## [0.6.0] - [09 November 2015]

### Features
- [Upgrade Mesos support to 0.25.0](https://github.com/mesos/elasticsearch/issues/338)

### Enhanced
- [Test Authentication in system tests](https://github.com/mesos/elasticsearch/issues/304)
- [Test JAR-mode (no docker) in system tests](https://github.com/mesos/elasticsearch/issues/354)

### Fixed
- [Resolving executor IP address when using jar](https://github.com/mesos/elasticsearch/issues/388)
- [Use hostname, use system to resolve hostnames](https://github.com/mesos/elasticsearch/issues/392)

## [0.5.2] - [29 October 2015]

### Fixed
- [Executor ip addresses are not available](https://github.com/mesos/elasticsearch/issues/390)


## [0.5.1] - [27 October 2015]

### Enhanced
- [Migrate ES system tests to new MiniMesos API](https://github.com/mesos/elasticsearch/issues/359)
- System test hardening and refactoring (various)
- Cleaned debug logging (various)
- [Executor should report its own IP address to scheduler refactoring](https://github.com/mesos/elasticsearch/issues/362)

### Fixed
- [TASK_KILLED was not considered an error state, so executors would not be removed](https://github.com/mesos/elasticsearch/issues/369)
- [GUI configuration NPE due to new jar options](https://github.com/mesos/elasticsearch/issues/357)
- [Remove 'mesos/elasticsearch-base' from docker push on release](https://github.com/mesos/elasticsearch/issues/356)

## [0.5.0] - [19 October 2015]

### Enhanced
- [Implement Mesos Authorisation](https://github.com/mesos/elasticsearch/issues/218)
- [Allow users to run without docker](https://github.com/mesos/elasticsearch/issues/334)
- [Shrink docker images](https://github.com/mesos/elasticsearch/issues/348)
- [Replace JVM with OpenJDK runtime](https://github.com/mesos/elasticsearch/issues/347)

### Fixed
- [Fix readthedocs documentation links to github bug documentation](https://github.com/mesos/elasticsearch/issues/341)
- [Refactor scheduler to further testing refactoring](https://github.com/mesos/elasticsearch/issues/327)
- [Scheduler Search API endpoint returns "java.util.NoSuchElementException" API bug](https://github.com/mesos/elasticsearch/issues/311)

## [0.4.3] - [28 September 2015]

### Enhanced
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