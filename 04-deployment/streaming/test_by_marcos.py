
import lambda_function_by_marcos

event = {
  "Records": [
    {
      "kinesis": {
        "kinesisSchemaVersion": "1.0",
        "partitionKey": "1",
        "sequenceNumber": "49630460813364010859294112709264948506227419919218114562",
        "data": "eyAKICAgICAgICAicmlkZSI6IHsgCiAgICAgICAgICAgICJQVUxvY2F0aW9uSUQiOiAxMzAsIAogICAgICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1LCAKICAgICAgICAgICAgInRyaXBfZGlzdGFuY2UiOiAzLjY2IAogICAgICAgIH0sICAKICAgICAgICAicmlkZV9pZCI6IDE1NiAKICAgIH0=",
        "approximateArrivalTimestamp": 1655226920.183
      },
      "eventSource": "aws:kinesis",
      "eventVersion": "1.0",
      "eventID": "shardId-000000000000:49630460813364010859294112709264948506227419919218114562",
      "eventName": "aws:kinesis:record",
      "invokeIdentityArn": "arn:aws:iam::546106488772:role/lambda-kinesis-role",
      "awsRegion": "eu-west-1",
      "eventSourceARN": "arn:aws:kinesis:eu-west-1:546106488772:stream/ride_events"
    }
  ]
}


result = lambda_function_by_marcos.lambda_handler(event, None)
print(result)
