#!/bin/bash

/usr/local/bin/wrapdocker

docker load < /tmp/build/images/executor.tar

mesos-slave