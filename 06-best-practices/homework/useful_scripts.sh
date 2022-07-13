pytest ./test -s
aws --endpoint-url="http://localhost:4566" s3 ls
aws --endpoint-url="http://localhost:4566" s3 mb s3://nyc-duration
aws s3 ls s3://nyc-duration --endpoint-url=http://localhost:4566  --recursive --human-readable


export INPUT_FILE_PATTERN=s3://nyc-duration/fhv_tripdata_{year:04d}-{month:02d}.parquet
export OUTPUT_FILE_PATTERN=s3://nyc-duration/fhv_tripdata_{year:04d}-{month:02d}.parquet
