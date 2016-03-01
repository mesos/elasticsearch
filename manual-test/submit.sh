#!/bin/bash

cat $1 | sed -e 's/$MASTER/'"$MASTER"'/' | sed -e 's/$SLAVE0/'"$SLAVE0"'/' | curl -XPOST -H 'Content-Type:application/json' -d @- http://$MASTER:8080/v2/apps