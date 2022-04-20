#!/bin/sh

set -e

if [ ! -z "$ONLY_BUILTINS" ]; then
    exit 0
fi

    PORT=6379
REDIS_PASSWORD=thehfhghedhdjfhgfhdhdhdf
echo $(hostname):$PORT $REDIS_PASSWORD > ~/.ray-test-72363726-details

GIGABYTE=1000000000  # close enough

ray stop  # in case there's an a leftover ray
ray start --head --port=$PORT --redis-password=$REDIS_PASSWORD --memory $GIGABYTE --object-store-memory $GIGABYTE --redis-max-memory $GIGABYTE $RAY_START_EXTRAS

RAY_HEAD_FILE=~/.ray-test-72363726-details PARAMSURVEY_BACKEND=ray pytest "$@"

ray stop

