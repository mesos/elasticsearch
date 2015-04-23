# elasticsearch
*Coming soon!* Elasticsearch on Mesos

# Getting Started

This framework requires a running <a href="http://mesos.apache.org">Mesos</a> cluster with <a href="https://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html">HDFS</a>.
The use of <a href="https://github.com/mesosphere/marathon">Marathon</a> is optional.

The framework can be run by building the code, the docker images, transferring the code to the Mesos cluster and launching the framework <i>scheduler</i>.

## Full steps to build in Mac

Actions to perform to start in Mac:

 1. In terminal go to your project directory:
	> $ cd project_directory

 2. Start Vagrant:
	> $ vagrant up
	
 3. When completed the installation/startup then SSH:
	> $ vagrant ssh
	
 4. Get to project directory in VM:
	> $ sudo -i
	
	> $ cd /vagrant

 5. Start the build with Gradle:
	>$ gradlew build

If you get the following failure 
>What went wrong:
Execution failed for task ':elasticsearch-cloud-mesos:buildDockerImage'. 
\> org.newsclub.net.unix.AFUNIXSocketException: No such file or directory (socket: /run/docker.sock)

then run the following command to make sure that docker is running:
	> $ systemctl restart docker

and finally run gradlew build again:
	> $ gradlew build

## How to build

> $ gradlew build

Optionally run <a href="https://github.com/dougborg/gdub">gdub</a> which runs the gradle wrapper from any subdirectory.

## How to install on Mesos

Add a host entry to your /etc/hosts which is called 'master' and points to your Mesos master. Now run

> $ deploy.sh

This script performs the following actions:

* Transfers the <i>scheduler</i> jar to the master node
* Transfers the <i>executor</i> jar to the master node
* Puts the <i>executor</i> jar in HDFS under /elasticsearch-mesos
* Transfers the <i>elasticsearch-cloud-mesos</i> zip to the master node
* Puts the zip in HDFS under /elasticsearch-mesos

After the script has run now you can start the <i>scheduler</i> by SSHing into the master and running:

> $ java -jar elasticsearch-mesos-scheduler.jar -m MASTER_IP:5050 -n 3 -nn MASTER_IP:8020

## How to install on Marathon

Run the deploy.sh script from the root directory to install all the components. Now change to the scheduler folder and run 

> $ ./deploy-to-marathon.sh 

This scripts loads the marathon.json file and runs the scheduler in a container on one of the slaves. Note that it 
requires host networking.

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
