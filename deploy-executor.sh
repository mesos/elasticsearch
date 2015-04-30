#!/bin/bash

for master in $MASTERS;
do
    scp executor/build/libs/elasticsearch-mesos-executor.jar ${master}:.
    ssh ${master} "hdfs dfs -put -f elasticsearch-mesos-executor.jar /elasticsearch"
    break
    #TODO: Make sure we stop after first sucessfull deployment
done



