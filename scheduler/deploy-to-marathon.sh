#!/usr/bin/env bash
curl -sk -XPOST -d @marathon.json -H "Content-Type: application/json" http://master:8080/v2/apps