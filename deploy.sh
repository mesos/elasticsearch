#!/bin/bash

for i in "$@"
do
case $i in
    -m=*|--master=*)
    CLI_MASTERS="${CLI_MASTERS} ${i#*=}"
    shift
    ;;
    -s=*|--slave=*)
    CLI_SLAVES="${CLI_SLAVES} ${i#*=}"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done
export MASTERS=${CLI_MASTERS-${MASTERS:-master}}
export SLAVES=${CLI_SLAVES-${SLAVES:-slave1 slave2 slave3}}

echo Will deploy to the following masters: $MASTERS
echo Will deploy to the following slaves: $SLAVES

gw clean build -x test

./deploy-executor.sh &
./deploy-cloud-mesos.sh &

wait

docker push mesos/elasticsearch-scheduler
docker push mesos/elasticsearch-cloud-mesos
for slave in $SLAVES;
do
    ssh $slave "docker pull mesos/elasticsearch-scheduler"
    ssh $slave "docker pull mesos/elasticsearch-cloud-mesos"
done

wait

cd scheduler; ./deploy-to-marathon.sh
