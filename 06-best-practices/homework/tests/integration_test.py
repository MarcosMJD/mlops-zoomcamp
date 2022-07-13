import os
import pandas as pd
from tests.test_batch import prepare_data

S3_ENDPOINT_URL = "http://localhost:4566/"
INPUT_FILE_PATTERN='s3://nyc-duration/fhv_tripdata_{year:04d}-{month:02d}.parquet'
OUTPUT_FILE_PATTERN='s3://nyc-duration/fhv_tripdata_predictions_{year:04d}-{month:02d}.parquet'
YEAR=2021
MONTH=1

def test_integration():

    df_input =prepare_data()

    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
    }

    input_url = f's3://nyc-duration/fhv_tripdata_{YEAR:04d}-{MONTH:02d}.parquet'

    df_input.to_parquet(
        input_url,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

    print(df_input)
    print(f'Calling batch_by_marcos.py with params: year={YEAR}, month={MONTH}')
    os.environ['INPUT_FILE_PATTERN'] = INPUT_FILE_PATTERN
    os.environ['OUTPUT_FILE_PATTERN'] = OUTPUT_FILE_PATTERN
    os.environ['S3_ENDPOINT_URL'] = S3_ENDPOINT_URL

    print(f'and env vars: {INPUT_FILE_PATTERN}, {OUTPUT_FILE_PATTERN}, {S3_ENDPOINT_URL}')
    command = f"python batch_by_marcos.py {YEAR} {MONTH}"
    os.system(command)

    output_url = OUTPUT_FILE_PATTERN.format(year=YEAR, month=MONTH)
    df_results = pd.read_parquet(output_url, storage_options=options)
    predicted_result = df_results["predicted_duration"].sum()
    print(f'Sum of predicted durations = {predicted_result}')
    expected_result = 69.28869683240714
    print(f'expected_result = {expected_result}')
    print(f'difference = {abs(predicted_result-expected_result)}')

    assert abs(predicted_result-expected_result) < 0.01




