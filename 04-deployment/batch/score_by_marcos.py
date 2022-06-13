import sys
import uuid
import pandas as pd
import mlflow
from sklearn.metrics import mean_squared_error
import os


experiment_uri = 's3://mlops-bucket-mmjd/2'

def read_dataframe(filename: str):
    
    df = pd.read_parquet(filename)
    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']
    df['ride_id'] = df.apply(lambda x: str(uuid.uuid4()), axis = 1)
    
    return df


def prepare_dictionaries(df: pd.DataFrame):

    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')

    return dicts


def load_model(run_id):

  logged_model = f'{experiment_uri}/{run_id}/artifacts/model'
  model = mlflow.pyfunc.load_model(logged_model)

  return model
    
def make_result (df, y_pred, run_id):
    
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['lpep_pickup_datetime'] = df['lpep_pickup_datetime']
    df_result['PULocationID'] = df['PULocationID']
    df_result['DOLocationID'] = df['DOLocationID']
    df_result['actual_duration'] = df['duration']
    df_result['predicted_duration'] = y_pred
    df_result['diff'] = df_result['actual_duration'] - df_result['predicted_duration']
    df_result['model_version'] = run_id

    return df_result

def apply_model (run_id, input_file, output_file):
    df = read_dataframe(input_file)
    dicts = prepare_dictionaries(df)
    model = load_model(run_id)
    y_pred = model.predict(dicts)
    df_result = make_result(df, y_pred, run_id)
    df_result.to_parquet(output_file)


def run():
    
    taxi_type = sys.argv[1]
    year = int(sys.argv[2])
    month = int(sys.argv[3])
    run_id = sys.argv[4]

    """taxi_type = 'green'
    year = 2021
    month = 2
    run_id = '6574d0d7c4b044f585c08de11b3582c4'"""

    output_folder = f'./output/{taxi_type}/'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    input_file = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'{output_folder}/{year:04d}-{month:02d}.parquet'

    apply_model (
       run_id = run_id,
       input_file = input_file,
       output_file = output_file)

if __name__ == '__main__':
    run()
