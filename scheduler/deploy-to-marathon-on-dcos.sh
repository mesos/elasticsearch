#!/usr/bin/env bash

aws_masters=(
    ec2-52-16-108-67.eu-west-1.compute.amazonaws.com
)

for master in ${aws_masters[@]}; do
		echo "Removing any existing version of the application via Marathon"

    curl -k -XDELETE -H "Content-Type: application/json" http://${master}:8080/v2/apps/elasticsearch-mesos-scheduler?force=true

#    sleep 2

#    curl -k -XPOST -d @marathon.json -H "Content-Type: application/json" http://${master}:8080/v2/apps
#    break
done
