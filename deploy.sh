#!/bin/bash

gw clean build -x test

./deploy-executor.sh &
./deploy-cloud-mesos.sh &

wait

docker push mesos/elasticsearch-scheduler
ssh slave1 "docker pull mesos/elasticsearch-scheduler" &
ssh slave2 "docker pull mesos/elasticsearch-scheduler" &
ssh slave3 "docker pull mesos/elasticsearch-scheduler" &

wait

cd scheduler; ./deploy-to-marathon.sh
