#!/bin/bash

# Start Docker & Mesos-Slave

supervisord -c /etc/supervisor.conf &

while ! supervisorctl status docker | grep -q 'EXITED'; do sleep 1; done
