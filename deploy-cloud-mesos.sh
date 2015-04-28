#!/bin/bash

scp elasticsearch-cloud-mesos/build/docker/elasticsearch-cloud-mesos.zip master:
ssh master "hdfs dfs -put -f elasticsearch-cloud-mesos.zip /elasticsearch"


