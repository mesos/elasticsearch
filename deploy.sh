#!/bin/bash

scp executor/build/libs/elasticsearch-mesos-executor.jar master:
ssh master "hdfs dfs -put -f elasticsearch-mesos-executor.jar /elasticsearch"

scp scheduler/build/docker/elasticsearch-mesos-scheduler.jar master:

scp elasticsearch-cloud-mesos/build/docker/elasticsearch-cloud-mesos.zip master:
ssh master "hdfs dfs -put -f elasticsearch-cloud-mesos.zip /elasticsearch"

