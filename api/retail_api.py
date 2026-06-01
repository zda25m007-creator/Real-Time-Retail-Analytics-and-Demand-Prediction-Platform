from flask import Flask, request, jsonify
import joblib
import pandas as pd
import numpy as np
import os

# Load model
bundle = joblib.load('model_bundle.joblib')
model = bundle['model']
features = bundle['features']
model_name = bundle['model_name']

app = Flask('retail_demand_api')

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    df = pd.DataFrame([data])
    for f in features:
        if f not in df.columns:
            df[f] = 0
    pred = model.predict(df[features])
    qty = float(np.clip(pred[0], 0, None))
    return jsonify({'predicted_quantity': round(qty, 2), 'model': model_name, 'status': 'ok'})

@app.route('/batch_predict', methods=['POST'])
def batch_predict():
    data = request.get_json()
    df = pd.DataFrame(data)
    for f in features:
        if f not in df.columns:
            df[f] = 0
    preds = model.predict(df[features])
    return jsonify([{'predicted_quantity': round(float(np.clip(p, 0, None)), 2)} for p in preds])

@app.route('/health', methods=['GET', 'POST'])
def health():
    return jsonify({'status': 'healthy', 'model': model_name, 'service': 'retail_demand_api'})

@app.route('/model_info', methods=['GET'])
def model_info():
    return jsonify({'model_name': model_name, 'features': features, 'target': 'TotalQuantity'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001)
