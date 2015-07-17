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
- [x] Rapid code + test (Docker compose)
- [x] Build automation (Gradle)

### User tools
- [ ] One click DCOS install
- [ ] One JSON post to marathon install

### Certification

- [ ] DCOS Certified

# Getting Started

We recommend that users install via marathon or via the DCOS command line (coming soon!).

This framework requires:
* A running [Mesos](http://mesos.apache.org) cluster
* The use of <a href="https://github.com/mesosphere/marathon">Marathon</a> is strongly recommended to provide resiliency against scheduler failover.

# Users

## How to install on Marathon

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
  "args": ["-n", "3", "-zk", "zk://ZOOKEEPER_IP_ADDRESS:2181/mesos"],
  "cpus": 0.2,
  "mem": 512.0,
  "instances": 1
}
```

Then post to marathon to instantiate the scheduler:
`curl -k -XPOST -d @marathon.json -H "Content-Type: application/json" http://MARATHON_IP_ADDRESS:8080/v2/apps`

## User Interface

The web based user interface is available on port 8080 of the scheduler by default. It displays real time information about the tasks running in the cluster and a basic configuration overview of the cluster. 

The user interface uses REST API of the Elasticsearch Mesos Framework. You can find the API documentation here: [docs.elasticsearchmesosui.apiary.io](http://docs.elasticsearchmesosui.apiary.io/).


# Developers

For developers, we have provided a range of tools for testing and running the project. Check out the [mini-mesos](https://github.com/containersolutions/mini-mesos) project for an in-memory Mesos cluster for integration testing.

## Quickstart

You can run Mesos-Elasticsearch using <a href="https://github.com/containersolutions/mini-mesos">Mini Mesos</a>, a containerized Mesos cluster for testing frameworks.

### How to run on Linux

#### Requirements

* Docker

```
$ ./gradlew build docker system-test:main
```

### How to run on Mac 

#### Requirements

* Docker Machine

```
$ docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-cpu-count 2 mesos-es
$ eval $(docker-machine env mesos-es)
$ ./gradlew build docker system-test:main
```

## How to build scheduler and executor Docker containers

This describes how to build and launch a local instance of mesos, with the Mesos Elasticsearch project installed. If you want to build and run the containers natively, then skip the docker-machine step.

### Launching a docker-machine VM

If you want to run docker-compose in a virtual machine (for example you are on a mac, where the native mesos libraries don't work), then you can use docker machine.
* Install docker-machine: https://docs.docker.com/machine/#installation
* Create a virtual machine: ```$ docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-cpu-count 2 dev```
* Export the environment variables so you can communicate with the docker daemon: ```$ eval "$(docker-machine env dev)"```

Docker-compose will connect to the VM docker daemon that was exported above.

### Building the code and containers

The docker containers for the scheduler and executor are not built by default:

```
$ ./gradlew build buildDockerImage
```

### Building the containers

Build only the scheduler or executor Docker container:

```
$ ./gradlew :scheduler:docker
$ ./gradlew :executor:docker
```

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
