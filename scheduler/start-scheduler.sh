#!/bin/bash
exec java $JAVA_OPTS -Djava.library.path=/usr/lib -jar /tmp/mesos-elasticsearch-scheduler.jar "$@"
