#!/bin/bash
set -x

ssh -i $KEY ubuntu@$SLAVE0 'sudo docker kill $(sudo docker ps -a -q) ; sudo docker rm `sudo docker ps --no-trunc -aq`' 
ssh -i $KEY ubuntu@$SLAVE1 'sudo docker kill $(sudo docker ps -a -q) ; sudo docker rm `sudo docker ps --no-trunc -aq`'
ssh -i $KEY ubuntu@$SLAVE2 'sudo docker kill $(sudo docker ps -a -q) ; sudo docker rm `sudo docker ps --no-trunc -aq`'
