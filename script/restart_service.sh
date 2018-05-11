#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
docker-compose -f $SCRIPT_DIR/../docker-compose-CeleryExecutor.yml restart $1
