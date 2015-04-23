# elasticsearch
*Coming soon!* Elasticsearch on Mesos

# Getting Started

This framework requires a running <a href="http://mesos.apache.org">Mesos</a> cluster
with <a href="https://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html">HDFS</a>.
The use of <a href="https://github.com/mesosphere/marathon">Marathon</a> is optional.
The framework can be run by building the code, the docker images, transferring the code to the Mesos cluster and
launching the framework <i>scheduler</i>.

## How to build with vagrant

> Known to work on Vagrant 1.7.2 and VirtualBox 4.3.26

> You can save yourself some time by first checking that the docker service is running with
> `systemctl status docker`. If it is not, you can start it using `sudo systemctl start docker`

> Currently you need to sudo the build command or the Docker part will fail. This will be fixed shortly.

The following will build everything.

````
$ vagrant up
$ vagrant ssh
$ cd /vagrant
$ sudo ./gradlew build
````

You can also build sub-components by remembering to use a relative path to the gradle wrapper. If you don't want to deal with relative paths you can run <a href="https://github.com/dougborg/gdub">gdub</a> which runs the gradle wrapper from any subdirectory.

````
$ cd /vagrant
$ cd commons
$ sudo ../gradlew build
````

## How to install on Mesos

Add a host entry to your /etc/hosts which is called 'master' and points to your Mesos master. Now run

````
$ deploy.sh
````

This script performs the following actions:

* Transfers the <i>scheduler</i> jar to the master node
* Transfers the <i>executor</i> jar to the master node
* Puts the <i>executor</i> jar in HDFS under /elasticsearch-mesos
* Transfers the <i>elasticsearch-cloud-mesos</i> zip to the master node
* Puts the zip in HDFS under /elasticsearch-mesos

After the script has run now you can start the <i>scheduler</i> by SSHing into the master and running:

````
$ java -jar elasticsearch-mesos-scheduler.jar -m MASTER_IP:5050 -n 3 -nn MASTER_IP:8020
````

## How to install on Marathon

Run the deploy.sh script from the root directory to install all the components. Now change to the scheduler folder and run 

````
$ ./deploy-to-marathon.sh 
````

This scripts loads the marathon.json file and runs the scheduler in a container on one of the slaves. Note that it 
requires host networking.

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
