#!/bin/bash

# contains(string, substring)
#
# Returns 0 if the specified string contains the specified substring,
# otherwise returns 1.
contains() {
    string="$1"
    substring="$2"
    if [[ debug == "$3" ]]; then
        echo "Does $string contain $substring?"
    fi
    if test "${string#*$substring}" != "$string"
    then
        echo "PASS"
        return 0    # $substring is in $string
    else
        echo "FAIL"
        return 1    # $substring is not in $string
    fi
}

contains "$(curl -s $SLAVE0:31000)" Search $1
contains "$(curl -s $SLAVE1:31000)" Search $1
contains "$(curl -s $SLAVE2:31000)" Search $1
contains "$(curl -s $SLAVE0:31000/_cat/nodes | wc -l)" "3" $1 # Three nodes in cluster

# Import data
ssh -o StrictHostKeyChecking=no -i $KEY ubuntu@$SLAVE0 'sudo docker run --rm -e ELASTIC_SEARCH_URL=http://'"$SLAVE0"':31000 mwldk/shakespeare-import'

contains "$(curl -s $SLAVE0:31000/_search?q=beans)" "Peas and beans are as dank here as a dog" $1