#!/usr/bin/env bash
curl -k -XDELETE -H "Content-Type: application/json" http://master:8080/v2/apps//elasticsearch-mesos-scheduler?force=true

sleep 2

curl -k -XPOST -d @marathon.json -H "Content-Type: application/json" http://master:8080/v2/apps
