# Elasticsearch
*Coming soon!* Elasticsearch on Mesos

# Roadmap

### Features

- [x] Deployment
- [x] Durable cluster topology (via ZooKeeper)
- [x] Web UI on scheduler port 8080
- [ ] Support deploying multiple Elasticsearch clusters to single Mesos cluster
- [ ] Fault tolerance
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
- [ ] One click DCOS install
- [ ] One JSON post to marathon install

### Certification

- [ ] DCOS Certified

## Quickstart

You can run Mesos-Elasticsearch using <a href="https://github.com/containersolutions/mini-mesos">Mini Mesos</a>, a containerized Mesos cluster for testing frameworks.

### How to run on Linux

## Requirements

* Docker

```
$ ./gradlew build docker system-test:main
```

### How to run on Mac 

## Requirements

* Docker Machine

```
$ docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-cpu-count 2 mesos-es
$ eval $(docker-machine env mesos-es)
$ ./gradlew build docker system-test:main
```

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
