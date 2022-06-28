#!/usr/bin/env python
# coding: utf-8

import pickle
import pandas as pd
import sys

def read_data(dataset_url='https://nyc-tlc.s3.amazonaws.com/trip+data', year=2021, month=2, categorical ='', numerical=''):
    
    ride_type = 'fhv_tripdata'
    file_uri = f'{dataset_url}/{ride_type}_{year:04d}-{month:02d}.parquet'

    df = pd.read_parquet(file_uri)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    return df


def load_model(path='model.bin'):
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    return dv, lr

def prepare_features (df, dv, categorical, numerical):
    
    dicts = df[categorical+numerical].to_dict(orient='records')
    X = dv.transform(dicts)
    return X

def predict (X, model):

    y = model.predict(X)
    return y

def save_predictions (df, y, path='predictions.parquet'):

    df_pred = pd.DataFrame()
    df_pred['ride_id'] = df['ride_id']
    df_pred['prediction'] = y
    df_pred.to_parquet(path=path, engine = 'pyarrow', index=False, compression='None')

def run_batch (year, month, model_path, output_path):
    categorical = ['PUlocationID', 'DOlocationID']
    numerical = []
    dataset_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data'
    dv, model = load_model(model_path)
    df = read_data(dataset_url, year, month, categorical, numerical)
    X = prepare_features(df, dv, categorical, numerical)
    y = predict(X, model)
    print(f'Mean predicted duration: {y.mean()}')
    save_predictions(df, y, path=output_path)

if __name__ == '__main__':
    
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2021
    month = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    model_path = sys.argv[3] if len(sys.argv) > 3 else 'model.bin'
    output_path = sys.argv[4] if len(sys.argv) > 4 else f'predictions-{year}-{month}.parquet'

    print(f'Calculating predictions for year {year:04d} and month {month:02d}')
    run_batch(year, month, model_path, output_path)


