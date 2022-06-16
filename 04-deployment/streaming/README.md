## Machine Learning for Streaming

* Scenario
* Creating the role 
* Create a Lambda function, test it
* Create a Kinesis stream
* Connect the function to the stream
* Send the records 

Links

* [Tutorial: Using Amazon Lambda with Amazon Kinesis](https://docs.amazonaws.cn/en_us/lambda/latest/dg/with-kinesis-example.html)

## Code snippets

Sending data:

```bash
KINESIS_STREAM_INPUT=ride_events
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data "Hello, this is a test."
```

Decoding base64

```python
base64.b64decode(data_encoded).decode('utf-8')
```

Record example

```json
{
    "ride": {
        "PULocationID": 130,
        "DOLocationID": 205,
        "trip_distance": 3.66
    }, 
    "ride_id": 123
}
```

Sending this record

```bash
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data '{
        "ride": {
            "PULocationID": 130,
            "DOLocationID": 205,
            "trip_distance": 3.66
        }, 
        "ride_id": 156
    }'
```

In Powershell use:
```
$env:KINESIS_STREAM_INPUT="ride_events"
aws kinesis put-record --cli-binary-format raw-in-base64-out `
    --stream-name $env:KINESIS_STREAM_INPUT `
    --partition-key 1 `
    --data '{ 
        \"ride\": { 
            \"PULocationID\": 130, 
            \"DOLocationID\": 205, 
            \"trip_distance\": 3.66 
        },  
        \"ride_id\": 156 
    }'
```


Test event:

```json
{
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49630081666084879290581185630324770398608704880802529282",
                "data": "ewogICAgICAgICJyaWRlIjogewogICAgICAgICAgICAiUFVMb2NhdGlvbklEIjogMTMwLAogICAgICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1LAogICAgICAgICAgICAidHJpcF9kaXN0YW5jZSI6IDMuNjYKICAgICAgICB9LCAKICAgICAgICAicmlkZV9pZCI6IDI1NgogICAgfQ==",
                "approximateArrivalTimestamp": 1654161514.132
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49630081666084879290581185630324770398608704880802529282",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::XXXXXXXXX:role/lambda-kinesis-role",
            "awsRegion": "eu-west-1",
            "eventSourceARN": "arn:aws:kinesis:eu-west-1:XXXXXXXXX:stream/ride_events"
        }
    ]
}
```


Reading from the stream

```bash
KINESIS_STREAM_OUTPUT='ride_predictions'
SHARD='shardId-000000000000'

SHARD_ITERATOR=$(aws kinesis \
    get-shard-iterator \
        --shard-id ${SHARD} \
        --shard-iterator-type TRIM_HORIZON \
        --stream-name ${KINESIS_STREAM_OUTPUT} \
        --query 'ShardIterator' \
)

RESULT=$(aws kinesis get-records --shard-iterator $SHARD_ITERATOR)

echo ${RESULT} | ./jq-win64.exe -r '.Records[0].Data' | base64 --decode
``` 

Running the test

```bash
export PREDICTIONS_STREAM_NAME="ride_predictions"
export RUN_ID="e1efc53e9bd149078b0c12aeaa6365df"
export TEST_RUN="True"

python test.py
```


Putting everything to Docker

```bash
docker build -t stream-model-duration:v1 .

docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="6574d0d7c4b044f585c08de11b3582c4" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="eu-west-1" \
    stream-model-duration:v1

docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="6574d0d7c4b044f585c08de11b3582c4" \
    -e TEST_RUN="True" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
    stream-model-duration:v1
```

URL for testing:

* http://localhost:8080/2015-03-31/functions/function/invocations


Creating an ECR repo

```bash
aws ecr create-repository --repository-name duration-model
```

Keep the uri of the repository so that docker can push the image later on. E.g.:
```
546106488772.dkr.ecr.eu-west-1.amazonaws.com/duration-model
```

Logging in

To log in to an Amazon ECR registry

get-login (deprecated
This command retrieves an authentication token using the GetAuthorizationToken API, and then it prints a docker login command with the authorization token and, if you specified a registry ID, the URI for an Amazon ECR registry. You can execute the printed command to authenticate to the registry with Docker. After you have authenticated to an Amazon ECR registry with this command, you can use the Docker CLI to push and pull images to and from that registry as long as your IAM principal has access to do so until the token expires. The authorization token is valid for 12 hours.

```bash
$(aws ecr get-login --no-include-email)
```

get-login-password
This command retrieves and displays an authentication token using the GetAuthorizationToken API that you can use to authenticate to an Amazon ECR registry. You can pass the authorization token to the login command of the container client of your preference, such as the Docker CLI. After you have authenticated to an Amazon ECR registry with this command, you can use the client to push and pull images from that registry as long as your IAM principal has access to do so until the token expires. The authorization token is valid for 12 hours.

```bash
aws ecr get-login-password \
    --region eu-west-1\
| docker login \
    --username AWS \
    --password-stdin 546106488772.dkr.ecr.eu-west-1.amazonaws.com
```

Pushing 

```bash
REMOTE_URI="546106488772.dkr.ecr.eu-west-1.amazonaws.com/duration-model"
REMOTE_TAG="v1"
REMOTE_IMAGE=${REMOTE_URI}:${REMOTE_TAG}

LOCAL_IMAGE="stream-model-duration:v1"
docker tag ${LOCAL_IMAGE} ${REMOTE_IMAGE}
docker push ${REMOTE_IMAGE}
```

Write down the ecr image uri (with tag):
```
546106488772.dkr.ecr.eu-west-1.amazonaws.com/duration-model:v1
```

