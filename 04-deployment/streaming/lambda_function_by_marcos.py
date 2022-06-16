import json
import base64
import boto3
import os
import mlflow

EXPERIMENT_URI = 's3://mlops-bucket-mmjd/2'
PREDICTIONS_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME','ride_predictions')
RUN_ID = os.getenv('RUN_ID', '6574d0d7c4b044f585c08de11b3582c4')
TEST_RUN = os.getenv('TEST_RUN', 'True') == 'True'

def load_model(run_id):

  logged_model = f'{EXPERIMENT_URI}/{RUN_ID}/artifacts/model'
  model = mlflow.pyfunc.load_model(logged_model)
  print(model)

  return model

model = load_model(RUN_ID)

def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features

kinesis_client = boto3.client('kinesis')

def predict(features):
    return float(model.predict(features)[0])
    
    
def lambda_handler(event, context):
    
    predictions = []
    
    # print(json.dumps(event))
    data_encoded = event['Records']
    for record in event['Records']:
        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        ride_event = json.loads(decoded_data)
        ride = ride_event['ride']
        ride_id = ride_event['ride_id']
        
        prediction = predict(prepare_features(ride))
        prediction_event = {
            'model': 'ride_duration_prediction_model',
            'version': '123',
            'prediction': {
                'ride_duration': prediction,
                'ride_id': ride_id,
            }
        }
        predictions.append(prediction_event)
        
        if not TEST_RUN:
          kinesis_client.put_record(
                  StreamName=PREDICTIONS_STREAM_NAME,
                  Data=json.dumps(prediction_event),
                  PartitionKey=str(ride_id)
              )
    
    return {
        'predictions': predictions
    }

