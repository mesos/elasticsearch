#!/bin/bash

scp executor/build/libs/elasticsearch-mesos-executor.jar master:
ssh master "hdfs dfs -put -f elasticsearch-mesos-executor.jar /elasticsearch"

