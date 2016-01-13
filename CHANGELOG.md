# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.7.0] - [13 January 2016]

shouldNotLoseDataWhenScalingDown ocasionally fails bug
#447 opened 5 days ago by philwinder  0.7.0
 2
		Minimesos hostnames do not point to running containers bug PeerReview testing
#445 opened 6 days ago by philwinder  0.7.0
@philwinder	 1
		Remove dependency on ES Zookeeper PeerReview refactoring
#439 opened 9 days ago by philwinder  0.7.0
@philwinder	 1
		Jenkins system test failure blocked testing
#436 opened 15 days ago by philwinder  0.7.0
 0
		Update to minimesos 0.5.0 build testing
#421 opened on Dec 11, 2015 by philwinder  0.7.0
@frankscholten	 1
		option to use IP addresses instead of hostname configuration
#418 opened on Dec 8, 2015 by sadovnikov  0.7.0
 3
		Update demo to use host mode, not bridge. documentation
#417 opened on Dec 1, 2015 by philwinder  0.7.0
 0
		Upgrade to elasticsearch 2.0+
#415 opened on Nov 25, 2015 by philwinder  0.7.0
@philwinder	 2
		Allow a user to specify his own http/transport ports enhancement PeerReview
#413 opened on Nov 19, 2015 by philwinder  0.7.0
 0
		Sanity check for framework registration before getting cluster state PeerReview
#410 opened on Nov 17, 2015 by philwinder  0.7.0
 0

- [Jitpack cannot build jar beacuse of docker build ](https://github.com/mesos/elasticsearch/issues/408)

- [Manual horizontal scaling](https://github.com/mesos/elasticsearch/issues/403)


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