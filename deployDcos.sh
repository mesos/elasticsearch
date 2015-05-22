#!/bin/bash

path='export PATH=$PATH:/opt/mesosphere/bin;'
java_home='export JAVA_HOME=/opt/mesosphere/active/java/usr/java;'

for i in "$@"
do
case $i in
    -m=*|--master=*)
    CLI_MASTERS="${CLI_MASTERS} ${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done
export MASTERS=${CLI_MASTERS-${MASTERS:-master}}

echo Will deploy to the following masters: $MASTERS

for master in $MASTERS;
do
    scp executor/build/libs/elasticsearch-mesos-executor.jar $master:

    ssh $master "$path $java_home hadoop fs -mkdir hdfs://hdfs/elasticsearch-mesos"
    ssh $master "$path $java_home hadoop fs -put -f elasticsearch-mesos-executor.jar hdfs://hdfs/elasticsearch-mesos/elasticsearch-mesos-executor.jar"

    scp scheduler/build/docker/elasticsearch-mesos-scheduler.jar $master:.

    scp elasticsearch-cloud-mesos/build/docker/elasticsearch-cloud-mesos.zip $master:
    ssh $master "$path $java_home hadoop fs -put -f elasticsearch-cloud-mesos.zip hdfs://hdfs/elasticsearch-mesos/elasticsearch-cloud-mesos.zip"
done
