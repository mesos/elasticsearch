#!/bin/bash

for master in $MASTERS;
do
    scp elasticsearch-cloud-mesos/build/docker/elasticsearch-cloud-mesos.zip ${master}:.
    ssh ${master} "hdfs dfs -put -f elasticsearch-cloud-mesos.zip /elasticsearch"
    break
    #TODO: Make sure we stop after first sucessfull deployment
done

