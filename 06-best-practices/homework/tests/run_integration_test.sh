#!/usr/bin/env bash
cd "$(dirname "$0")"
# To allow batch.py to find the model when running the tests
cd ..

docker-compose -f ./tests/docker-compose.yml up -d

sleep 5

aws --endpoint-url="http://localhost:4566" s3 mb s3://nyc-duration

pytest ./tests/integration_test.py -s

ERROR_CODE=$?

if [ ${ERROR_CODE} != 0 ]; then
    docker-compose  -f ./tests/docker-compose.yml down --volumes
    exit ${ERROR_CODE}
fi

docker-compose  -f ./tests/docker-compose.yml down --volumes 