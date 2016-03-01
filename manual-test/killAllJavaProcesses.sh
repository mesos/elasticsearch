#!/bin/bash

ssh -i $KEY ubuntu@$SLAVE0 "ps -ax | grep java | grep -v grep | awk '{ printf \$1 }' | xargs sudo kill"
ssh -i $KEY ubuntu@$SLAVE1 "ps -ax | grep java | grep -v grep | awk '{ printf \$1 }' | xargs sudo kill"
ssh -i $KEY ubuntu@$SLAVE2 "ps -ax | grep java | grep -v grep | awk '{ printf \$1 }' | xargs sudo kill"

