import os
import pickle

import mlflow
from flask import Flask, request, jsonify

RUN_ID = os.getenv('RUN_ID', '6574d0d7c4b044f585c08de11b3582c4')

# Load the model from the tracking server

# MLFLOW_TRACKING_URI = 'http://18.200.196.114:5000'
# mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
# model = mlflow.pyfunc.load_model(f'runs:/{RUN_ID}/model')

# Note that when generating the model with the random-forest.ipynb, a pipeline is used, so the model logged is the pipeline, which
# includes the dv. 
# If we had not done it that way, we would have to download the artifact (dv) with mlflow client and use it. Tedious.

# Instead of using the tracking server to retrieve the model, we use directly the artifact storage, in this case is S3 bucket.
# Avoiding using the tracking server. 

logged_model = f's3://mlops-bucket-mmjd/2/{RUN_ID}/artifacts/model'

# If we want to use model registry, we need the tracking server:
# We may pass the connections parameters via env vars, specially when using docker
# MLFLOW_TRACKING_URI = os.getenv('RUN_ID','http://18.200.196.114:5000')
MODEL_NAME = os.getenv('MODEL_NAME','green-taxi-duration-RandomForest')
STAGE = os.getenv('STAGE','production')
# mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
#logged_model = f'models:/{MODEL_NAME}/{STAGE}'

model = mlflow.pyfunc.load_model(logged_model)

def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    result = {
        'duration': pred,
        'model_version': MODEL_NAME+'-'+STAGE
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9000)
