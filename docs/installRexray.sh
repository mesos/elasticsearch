#!/bin/bash

if [ -z "$TF_VAR_access_key" ]; then
    echo "TF_VAR_access_key is empty"
    exit 1
fi

if [ -z "$TF_VAR_secret_key" ]; then
    echo "TF_VAR_secret_key is empty"
    exit 1
fi

# Install rexray
# ssh -i $KEY ubuntu@$1 'curl -sSL https://dl.bintray.com/emccode/rexray/install | sh -'
ssh -i $KEY ubuntu@$1 'curl -sSL https://dl.bintray.com/emccode/rexray/install | sh -s staged'

# Copy config to remote
scp -i $KEY scripts/config.yml ubuntu@$1:~

# Add AWS credentials. Guard against forward slashes in secret
ssh -i $KEY ubuntu@$1 'export MYVAR='"'$(echo $TF_VAR_access_key | sed -e 's/[\/&]/\\&/g')'"'; sed -i s/TF_VAR_access_key/$MYVAR/ config.yml'
ssh -i $KEY ubuntu@$1 'export MYVAR='"'$(echo $TF_VAR_secret_key | sed -e 's/[\/&]/\\&/g')'"'; sed -i s/TF_VAR_secret_key/$MYVAR/ config.yml'

# Move to correct directory
ssh -i $KEY ubuntu@$1 'sudo mv ~/config.yml /etc/rexray'

# Start rexray
ssh -i $KEY ubuntu@$1 'sudo rexray restart'

# Install dvdcli
ssh -i $KEY ubuntu@$1 'curl -sSL https://dl.bintray.com/emccode/dvdcli/install | sh -'

