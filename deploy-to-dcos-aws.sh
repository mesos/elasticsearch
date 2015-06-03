#!/usr/bin/env bash

# An Amazon AWS specific script for deploying the Elastic Search Mesos framework to DCOS.
#
# Requires:
#
# A working installation of the DCOS framework on an Amazon AWS cluster.
# A private key from Amazon AWS.
# An installation of the DCOS-CLI.
#
# Usage:
#
# ./deploy-to-dcos-aws.sh -i=../dcos/aws/ireland/dcos-elasticsearch.pem -m="ec2-52-16-108-67.eu-west-1" -s="ec2-54-76-242-98.eu-west-1 ec2-54-77-233-209.eu-west-1 ec2-54-77-234-27.eu-west-1 ec2-54-77-234-75.eu-west-1 ec2-54-77-55-250.eu-west-1"
#
# Where:
#	-i points to the private key associated with the account you are using on AWS to host DCOS.
#	-m is a space seperated list of master server nodenames.
#	-s is a space seperated list of slave server nodenames.

for i in "$@"
do
case $i in
    -i=*|--identity=*)
    CLI_IDENTITY="${CLI_IDENTITY} ${i#*=}"
    shift
    ;;
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

echo $CLI_IDENTITY

# Update the DCOS slaves with the latest Docker images.
for slave in ${CLI_SLAVES}; do
    echo "Pulling elasticsearch-scheduler Docker image on DCOS slave at $slave.compute.amazonaws.com"
		ssh -i $CLI_IDENTITY core@$slave.compute.amazonaws.com  "docker pull mesos/elasticsearch-scheduler"

    echo "Pulling elasticsearch-cloud-mesos Docker image on DCOS slave at $slave.compute.amazonaws.com"
		ssh -i $CLI_IDENTITY core@$slave.compute.amazonaws.com  "docker pull mesos/elasticsearch-cloud-mesos"
done

# Upload the build artifacts to the DCOS master/s.
for master in ${CLI_MASTERS}; do
    echo "Uploading elasticsearch-mesos-executor.jar file to DCOS master at $master.compute.amazonaws.com"
    scp -oStrictHostKeyChecking=no -i $CLI_IDENTITY executor/build/libs/elasticsearch-mesos-executor.jar core@${master}.compute.amazonaws.com:

    echo "Uploading elasticsearch-mesos-scheduler.jar file to DCOS master at $master.compute.amazonaws.com"
    scp -oStrictHostKeyChecking=no -i $CLI_IDENTITY scheduler/build/docker/elasticsearch-mesos-scheduler.jar core@${master}.compute.amazonaws.com:

    echo "Uploading elasticsearch-cloud-mesos.jar file to DCOS master at $master.compute.amazonaws.com"
    scp -oStrictHostKeyChecking=no -i $CLI_IDENTITY elasticsearch-cloud-mesos/build/docker/elasticsearch-cloud-mesos.zip core@${master}.compute.amazonaws.com:
done

./scheduler/deploy-to-marathon-on-dcos.sh

exit 0
