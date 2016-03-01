#!/bin/bash
set -x

ssh -i $KEY ubuntu@$SLAVE0 'sudo docker run --rm -e ELASTIC_SEARCH_URL=http://'"$SLAVE0"':31000 mwldk/shakespeare-import'

