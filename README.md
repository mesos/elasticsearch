# Elasticsearch
*Coming soon!* Elasticsearch on Mesos

# Getting Started

This framework requires:
* a running [Mesos](http://mesos.apache.org) cluster
* with <a href="https://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html">HDFS</a>.  The HDFS dependency is not at a code level.  It is used as  local repository for the elastic search executor.  This requirement will be removed over time but will likely remain is one of the options for executor fetching.
The use of <a href="https://github.com/mesosphere/marathon">Marathon</a> is optional.

The framework can be run by building the code, the docker images, transferring the code to the Mesos cluster and
launching the framework _scheduler_.

# How to build
```
$ ./gradlew build
```

Alteratively:
* Use [gdub](https://github.com/dougborg/gdub) which runs the gradle wrapper from any subdirectory, so that you don't need to deal with relative paths
* Use [Vagrant](#building-with-vagrant)
* Use [Docker](#building-with-docker)

## How to launch with Docker Compose

Build the project as described above

```
$ cd system-test/src/test/resources/mesos-es
$ docker-compose up
```

Now open the browser at http://localhost:5050 to view the Mesos GUI.

The Elasticsearch task can be accessed via the slave on port 9200. Find the IP address of the slave: 

```
$ docker ps # Check the container ID of the slave
$ docker inspect <ID> |  grep IPAddress # Find out the slave IP
```

Now open the browser at http://SLAVE_IP:9200 

When you are done with docker compose kill the containers and remove everything:

```
$ docker-compose kill 
$ docker-compose rm --force -v
```

## How to install on Mesos

```
$ deploy-executor.sh
$ deploy-scheduler.sh
$ deploy-cloud-mesos.sh
```
These scripts transfer the jars and the cloud-mesos zip to the master node. Also, the <i>executor</i> jar and 
cloud-mesos are put in HDFS onder /elasticsearch because they are used to launch the elasticsearch task. Now you can SSH
into the master node and run the <i>scheduler</i>

```bash
$ java -jar elasticsearch-mesos-scheduler.jar -m MASTER_IP:5050 -n 3 -nn MASTER_IP:8020
```

## How to install on Dcos

If you have followed the steps described in "Full steps to build on Mac" then  to deploy execute the following steps.

```bash
$ ./deployDcos.sh --master=MASTER_IP
```

Replace `MASTER_IP` in `--master=MASTER_IP` with a reference to a host recognisable by your `ssh` command.

## How to find theMesos master on AWS

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

Run the deploy.sh script from the root directory to install all the components. Now change to the scheduler folder and run 

```bash
$ ./deploy-to-marathon.sh 
```

This scripts loads the marathon.json file and runs the scheduler in a container on one of the slaves. Note that it 
requires host networking.

## How to import demo data

The [Sharekespeare dataset](http://www.elastic.co/guide/en/kibana/3.0/import-some-data.html) from Elastic.co can be
imported with the [mwldk/shakespeare-import](https://registry.hub.docker.com/u/mwldk/shakespeare-import/) Docker image.
Just point the `ELASTIC_SEARCH_URL` environment variable at one of your Elastic nodes.

```bash
$ docker run --rm -e ELASTIC_SEARCH_URL=http://${MASTER_IP}:9200 mwldk/shakespeare-import
```
## Alternative ways of building
### Building with Vagrant

Prerequisites:
* Running Docker service
* Vagrant 1.7.2 and VirtualBox 4.3.26 (at least the versions have been tested)

**Note:** Currently you need to sudo the build command or the Docker part will fail. This will be fixed shortly.

Actions to perform to start in Mac:

1. Start Vagrant from project directory:

    ```bash
    $ vagrant up
    ```

2. When completed SSH into the VM:

    ```bash
    $ vagrant ssh
    ```

3. Build 

    ```bash
    $ cd /vagrant
    $ sudo ./gradlew build
    ```
    
### Build performance
When building multi-project projects, you can force gradle to re-use the cached versions of previous libraries with the “-a” parameter. (see https://docs.gradle.org/current/userguide/multi_project_builds.html). The first time the build will take 45 minutes, but subsequent builds will be much faster with the -a.
E.g. sudo ./gradlew -a :scheduler:build -x :scheduler:buildDockerImage
Only takes 10 minutes on vagrant, rather than 45.

Also, you can improve speed by giving vagrant more CPUs and memory in the Vagrantfile.

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
