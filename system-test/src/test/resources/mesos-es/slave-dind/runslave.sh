#!/bin/bash

/usr/local/bin/wrapdocker

docker load < /tmp/executor.tar

mesos-slave