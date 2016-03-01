#!/bin/bash
set -x

if [ -z "$TF_VAR_access_key" ]; then
    echo "TF_VAR_access_key is empty"
    exit 1
fi

if [ -z "$TF_VAR_secret_key" ]; then
    echo "TF_VAR_secret_key is empty"
    exit 1
fi

# Install rexray and dvdcli
./scripts/installRexray.sh $1

# Download dvdi isolator
ssh -i $KEY ubuntu@$1 'sudo wget -P /usr/lib https://github.com/emccode/mesos-module-dvdi/releases/download/v0.4.1/libmesos_dvdi_isolator-0.25.0.so'

# Copy across module configuration settings
ssh -i $KEY ubuntu@$1 'echo { \"libraries\": [ { \"file\": \"/usr/lib/libmesos_dvdi_isolator-0.25.0.so\", \"modules\": [ { \"name\": \"com_emccode_mesos_DockerVolumeDriverIsolator\" } ] } ] } | sudo tee /usr/lib/dvdi-mod.json'
ssh -i $KEY ubuntu@$1 'echo file:///usr/lib/dvdi-mod.json | sudo tee /etc/mesos-slave/modules'
ssh -i $KEY ubuntu@$1 'echo com_emccode_mesos_DockerVolumeDriverIsolator | sudo tee /etc/mesos-slave/isolation'

# Restart mesos slave to load new module
ssh -i $KEY ubuntu@$1 'sudo service mesos-slave restart'
