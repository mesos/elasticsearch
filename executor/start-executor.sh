#!/bin/bash
if [ -z "$LIBPROCESS_IP" ]; then
    # Get the first non local IP address. Make sure you /etc/hosts file is correctly set.
    export LIBPROCESS_IP=$(hostname -I | awk '{ print $1}' | tr -d "\n")
fi
echo "LIBPROCESS_IP is set to '$LIBPROCESS_IP'";

java $JAVA_OPTS -Djava.library.path=/usr/local/lib -jar /tmp/elasticsearch-mesos-executor.jar $@