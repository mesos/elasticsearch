#!/bin/bash

path='export PATH=$PATH:/opt/mesosphere/bin;'
java_home='export JAVA_HOME=/opt/mesosphere/active/java/usr/java;'

scp -i ForkIDFrankfurt.pem executor/build/libs/elasticsearch-mesos-executor.jar core@master:

ssh -i ForkIDFrankfurt.pem  core@master "$path $java_home hadoop fs -mkdir hdfs://hdfs/elasticsearch-mesos"
ssh -i ForkIDFrankfurt.pem  core@master "$path $java_home hadoop fs -put -f elasticsearch-mesos-executor.jar hdfs://hdfs/elasticsearch-mesos/elasticsearch-mesos-executor.jar"

scp -i ForkIDFrankfurt.pem scheduler/build/docker/elasticsearch-mesos-scheduler.jar core@master:

scp -i ForkIDFrankfurt.pem elasticsearch-cloud-mesos/build/docker/elasticsearch-cloud-mesos.zip core@master:
ssh -i ForkIDFrankfurt.pem  core@master "$path $java_home hadoop fs -put -f elasticsearch-cloud-mesos.zip hdfs://hdfs/elasticsearch-mesos/elasticsearch-cloud-mesos.zip"

