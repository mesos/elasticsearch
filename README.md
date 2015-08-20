# Elasticsearch

[http://mesos.github.io/elasticsearch](http://mesos.github.io/elasticsearch)

  * [Roadmap](#roadmap)
    * [Features](#features)
    * [Depends on upcoming Mesos features](#depends-on-upcoming-mesos-features)
    * [Developer Tools](#developer-tools)
    * [User tools](#user-tools)
    * [Certification](#certification)
  * [Getting Started](#getting-started)
  * [Users Guide](#users-guide)
    * [How to install on Marathon](#how-to-install-on-marathon)
    * [User Interface](#user-interface)
  * [Developers Guide](#developers-guide)
    * [Quickstart](#quickstart)
    * [How to run on Linux](#how-to-run-on-linux)
      * [Requirements](#requirements)
    * [How to run on Mac](#how-to-run-on-mac)
      * [Requirements](#requirements-1)
    * [System test](#system-test)
      * [How to run system tests on Linux](#how-to-run-system-tests-on-linux)
        * [Requirements](#requirements-2)
      * [How to run on Mac](#how-to-run-on-mac-1)
        * [Requirements](#requirements-3)
    * [How to release](#how-to-release)
  * [Sponsors](#sponsors)
  * [License](#license)


## Roadmap

### Features

- [x] Deployment
- [x] Durable cluster topology (via ZooKeeper)
- [x] Web UI on scheduler port 8080
- [ ] Support deploying multiple Elasticsearch clusters to single Mesos cluster
- [x] Fault tolerance
- [ ] High availability (master, indexer, replica)
- [ ] Upgrading configuration
- [ ] Scale cluster horizontally
- [ ] Scale cluster vertically
- [ ] Upgrade
- [ ] Rollback
- [ ] Snapshot and restore

### Depends on upcoming Mesos features

- [ ] Faster task recovery with Mesos dynamic reservations (https://issues.apache.org/jira/browse/MESOS-1554)
- [ ] Persistent storage

### Developer Tools

- [x] Local environment (Docker-machine)
- [x] Rapid code + test (Mini Mesos)
- [x] Build automation (Gradle)

### User tools

- [x] One click DCOS install
- [x] One JSON post to marathon install

### Certification

- [ ] DCOS Certified

## Getting Started

We recommend that users install via marathon or via the DCOS command line (coming soon!).

This framework requires:
* A running [Mesos](http://mesos.apache.org) cluster
* The use of <a href="https://github.com/mesosphere/marathon">Marathon</a> is strongly recommended to provide resiliency against scheduler failover.

## Users Guide

### How to install on Marathon

Create a Marathon file like the one below and fill in the IP addresses and other configuration.

```
{
  "id": "elasticsearch-mesos-scheduler",
  "container": {
    "docker": {
      "image": "mesos/elasticsearch-scheduler",
      "network": "HOST"
    }
  },
  "args": ["-n", "3", "-zk", "zk://ZOOKEEPER_IP_ADDRESS:2181/mesos", "-ram", "2048"],
  "cpus": 0.2,
  "mem": 512.0,
  "env": {
    "JAVA_OPTS": "-Xms128m -Xmx256m"
  },
  "instances": 1
}
```

Then post to marathon to instantiate the scheduler:
`curl -k -XPOST -d @marathon.json -H "Content-Type: application/json" http://MARATHON_IP_ADDRESS:8080/v2/apps`

Note: the JAVA_OPTS line is required. If this is not set, then the Java heap space will be incorrectly set.

### User Interface

The web based user interface is available on port 8080 of the scheduler by default. It displays real time information about the tasks running in the cluster and a basic configuration overview of the cluster. 

The user interface uses REST API of the Elasticsearch Mesos Framework. You can find the API documentation here: [docs.elasticsearchmesosui.apiary.io](http://docs.elasticsearchmesosui.apiary.io/).

### Known issues
- Issue [#206](https://github.com/mesos/elasticsearch/issues/206): The GUI will not represent the true state of the cluster after a scheduler or executor reconciliation event. E.g. If the scheduler is killed and restarted, the GUI will show zero executors, even though there are executors present.
- Issue [#188](https://github.com/mesos/elasticsearch/issues/188): Database data IS NOT persisted to disk. Data storage is wholly reliant on cluster redundancy. This means that the framework is not yet recommended for production use.

## Developers Guide

For developers, we have provided a range of tools for testing and running the project. Check out the [mini-mesos](https://github.com/containersolutions/mini-mesos) project for an in-memory Mesos cluster for integration testing.

### Quickstart

You can run Mesos-Elasticsearch using <a href="https://github.com/containersolutions/mini-mesos">Mini Mesos</a>, a containerized Mesos cluster for testing frameworks.

### How to run on Linux

#### Requirements

* Docker

```
$ ./gradlew build system-test:main
```

### How to run on Mac 

#### Requirements

* Docker Machine

```
$ docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-cpu-count 2 mesos-es
$ eval $(docker-machine env mesos-es)
$ ./gradlew build buildDockerImage system-test:main
```

### System test

The project contains a system-test module which tests if the framework interacts correctly with Mesos, using <a href="https://github.com/containersolutions/mini-mesos">Mini Mesos</a>. We currently test Zookeeper discovery and the Scheduler's API by calling endpoints and verifying the results. As the framework grows we will add more system tests.

#### How to run system tests on Linux

##### Requirements

* Docker

```
$ ./gradlew build system-test:systemTest
```

#### How to run on Mac 

##### Requirements

* Docker Machine

```
$ docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-cpu-count 2 mesos-es
$ eval $(docker-machine env mesos-es)
$ ./gradlew build buildDockerImage system-test:systemTest
```

### How to release

Create the following Gradle property file in ~/.gradle/gradle.properties and refer to your Github and Docker Hub
user/pass.

```
systemProp.org.ajoberstar.grgit.auth.interactive.allow=false
systemProp.org.ajoberstar.grgit.auth.ssh.private=~/.ssh/id_rsa
systemProp.org.ajoberstar.grgit.auth.username=user
systemProp.org.ajoberstar.grgit.auth.password=password
dockerHubUsername=user
dockerHubPassword=******
dockerHubEmail=email
```

Now perform a release and specify the release type: major, minor or patch and your username.

```
$ ./gradlew build system-test:systemTest
```

If build and tests succeed release

```
$ ./gradlew release -PreleaseType={major,minor,patch} -PuserName={user}
```

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
