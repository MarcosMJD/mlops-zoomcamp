#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import os
from pathlib import Path

with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)

def read_data(filename: str):

    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    options = None
    if s3_endpoint_url is not None:
        options = {
            'client_kwargs': {
                'endpoint_url': s3_endpoint_url
            }
        }
    df = pd.read_parquet(filename, storage_options=options)

    return df

def save_result(df_result: pd.DataFrame, filename: str):

    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    options = None
    if s3_endpoint_url is not None:
        options = {
            'client_kwargs': {
                'endpoint_url': s3_endpoint_url
            }
        }
    df_result.to_parquet(filename, engine='pyarrow', index=False, storage_options=options)
    
    return

def prepare_data(df: pd.DataFrame, categorical: list):

    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df

def main (year: int, month: int, input_url: str, output_url: str):

    categorical = ['PUlocationID', 'DOlocationID']

    df = read_data(input_url)
    df = prepare_data(df, categorical)

    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    if output_url == 'local':
        data_folder = Path(__file__).parent / "data"
        output_url = f'{data_folder}/fhv_{year:04d}_{month:02d}.parquet'
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

    save_result(df_result, output_url)

def get_input_url(year: int, month: int):

    default_input_pattern = 'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def get_output_url(year: int, month: int):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

if __name__ == "__main__":

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2021
    month = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    input_url = get_input_url(year,month)
    output_url = get_output_url(year,month)

    main(year, month, input_url, output_url)
