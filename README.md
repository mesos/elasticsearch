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

This framework requires:
* A running [Mesos](http://mesos.apache.org) cluster
* The use of <a href="https://github.com/mesosphere/marathon">Marathon</a> is strongly recommended to provide resiliency against scheduler failover.

# How to build

```
$ ./gradlew build
```

Alteratively:
* Use [gdub](https://github.com/dougborg/gdub) which runs the gradle wrapper from any subdirectory, so that you don't need to deal with relative paths
* Use [Docker](#building-with-docker)
* Use [Docker-Machine](#launching-a-docker-machine-vm)

# How to build scheduler and executor Docker containers

This describes how to build and launch a local instance of mesos, with the Mesos Elasticsearch project installed. If you want to build and run the containers natively, then skip the docker-machine step.

## Launching a docker-machine VM

If you want to run docker-compose in a virtual machine (for example you are on a mac, where the native mesos libraries don't work), then you can use docker machine.
* Install docker-machine: https://docs.docker.com/machine/#installation
* Create a virtual machine: ```$ docker-machine create --driver virtualbox dev```
* Export the environment variables so you can communicate with the docker daemon: ```$ eval "$(docker-machine env dev)"```

Docker-compose will connect to the VM docker daemon that was exported above.

## Building the code

The docker containers for the scheduler and executor are not built by default:

```
$ ./gradlew build docker
```

## Building the containers

Build only the scheduler or executor Docker container:

```
$ ./gradlew :scheduler:docker
$ ./gradlew :executor:docker
```
## How to install on Mesos

Upload the scheduler jar to a Mesos node and run it. 

```bash
$ java -jar elasticsearch-mesos-scheduler.jar -n 3 -zk zk://ZOOKEEPER_IP_ADDRESS:2181/mesos
```

## How to find the Mesos master on AWS

1. Open Mesos website
	* Get the DCOS public DNS, 
	* Copy it into another browser instance and
	* Use port 5050 by adding at the end of the DNS public address :5050.
This will show the Mesos website for your instance
1. Select from the Mesos website the 'Slaves' view
1. Make node of all the 'Hosts' (slave hosts)
1. In AWS Services select 'EC2'
1. In the lefthand side menu select 'Instances'
1. Find the instance with a 'Public DNS' which is not any of the ones belonging to the slaves and which belong to the DCOS installation
	*  An instance belonging to the DCOS installation should have a 'Security groups' with a name that contains the word DCOS
1.  The 'Public DNS' for that instance is what you need to ssh (run the deploy script)

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

## How to import demo data

The [Sharekespeare dataset](http://www.elastic.co/guide/en/kibana/3.0/import-some-data.html) from Elastic.co can be
imported with the [mwldk/shakespeare-import](https://registry.hub.docker.com/u/mwldk/shakespeare-import/) Docker image.
Just point the `ELASTIC_SEARCH_URL` environment variable at one of your Elastic nodes.

```bash
$ docker run --rm -e ELASTIC_SEARCH_URL=http://${MASTER_IP}:9200 mwldk/shakespeare-import
```
## Alternative ways of building
### Building with Docker
```bash
$ docker run --rm -v /var/run/docker.sock:/var/run/docker.sock:rw \
      -v ~/.gradle/:/root/.gradle/:rw \
      -v `pwd`:/app:rw pierrevincent/gradle-java8 build
```

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
