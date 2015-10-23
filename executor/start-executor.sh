#!/bin/bash
LIBPROCESS_IP=$(ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}')
java $JAVA_OPTS -Djava.library.path=/usr/local/lib -jar /tmp/elasticsearch-mesos-executor.jar $@