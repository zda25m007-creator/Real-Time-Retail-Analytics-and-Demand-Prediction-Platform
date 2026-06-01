"""
train.py
Standalone ML Training Script
Z5008 Big Data Lab - IIT Madras Zanzibar
Author: Vineet Joshi | ZDA25M007
"""

import logging
import joblib
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.sklearn
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORAGE = {
    'AWS_ENDPOINT_URL':           'http://minio:9000',
    'AWS_ACCESS_KEY_ID':          'admin',
    'AWS_SECRET_ACCESS_KEY':      'bigdata123',
    'AWS_ALLOW_HTTP':             'true',
    'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    'AWS_REGION':                 'us-east-1'
}

FEATURE_COLS = [
    'Year', 'Month', 'WeekOfYear', 'DayOfWeek',
    'AvgUnitPrice', 'NumInvoices', 'UniqueCustomers',
    'TotalRevenue', 'StockCode_idx', 'Country_idx'
]
TARGET = 'TotalQuantity'
MODEL_PATH = '/home/jovyan/work/model_bundle.joblib'
EXPERIMENT_NAME = 'retail-demand-prediction-v2'


def load_features():
    """Load feature data from Delta Lake."""
    logger.info('Loading feature data from Delta Lake...')
    df = DeltaTable(
        's3://retail-v2/delta/features',
        storage_options=STORAGE
    ).to_pandas()
    logger.info(f'Features loaded: {len(df):,} rows')
    return df


def engineer_features(df):
    """Apply label encoding to categorical features."""
    logger.info('Engineering features...')

    le_stock   = LabelEncoder()
    le_country = LabelEncoder()

    df['StockCode_idx'] = le_stock.fit_transform(
        df['StockCode'].astype(str)
    )
    df['Country_idx'] = le_country.fit_transform(
        df['Country'].astype(str)
    )

    return df, le_stock, le_country


def train_model(model, model_name, X_train, X_test, y_train, y_test):
    """Train a single model and log to MLflow."""

    with mlflow.start_run(run_name=model_name):
        # Train
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # Metrics
        r2   = r2_score(y_test, y_pred)
        rmse = mean_squared_error(y_test, y_pred, squared=False)
        mae  = np.mean(np.abs(y_test - y_pred))

        # Log to MLflow
        mlflow.log_param('model_type', model_name)
        mlflow.log_param('n_features', len(FEATURE_COLS))
        mlflow.log_param('train_size', len(X_train))
        mlflow.log_param('test_size',  len(X_test))
        mlflow.log_metric('r2_score', r2)
        mlflow.log_metric('rmse', rmse)
        mlflow.log_metric('mae', mae)
        mlflow.sklearn.log_model(model, 'model')

        logger.info(
            f'{model_name}: R2={r2:.4f} | RMSE={rmse:.2f} | MAE={mae:.2f}'
        )

    return model, r2, rmse


def save_model_bundle(model, model_name, le_stock, le_country):
    """Save model bundle with encoders."""
    bundle = {
        'model':       model,
        'model_name':  model_name,
        'features':    FEATURE_COLS,
        'target':      TARGET,
        'le_stock':    le_stock,
        'le_country':  le_country,
        'trained_at':  datetime.now().isoformat(),
    }
    joblib.dump(bundle, MODEL_PATH)
    logger.info(f'Model bundle saved: {MODEL_PATH}')


def run_unit_tests(model):
    """Unit tests for model prediction function.
    Required: at least 2 non-trivial function tests.
    """
    logger.info('Running unit tests...')

    # Test 1: Prediction returns a positive number
    test_input = pd.DataFrame([{
        'Year': 2010, 'Month': 11, 'WeekOfYear': 46,
        'DayOfWeek': 3, 'AvgUnitPrice': 3.75,
        'NumInvoices': 5, 'UniqueCustomers': 4,
        'TotalRevenue': 185.0, 'StockCode_idx': 42,
        'Country_idx': 0
    }])
    pred = model.predict(test_input[FEATURE_COLS])[0]
    assert pred >= 0, f'Test 1 FAILED: prediction should be >= 0, got {pred}'
    logger.info(f'Test 1 PASSED: prediction = {pred:.2f} (positive)')

    # Test 2: Higher revenue should predict higher quantity
    low_rev = test_input.copy()
    low_rev['TotalRevenue'] = 50.0
    high_rev = test_input.copy()
    high_rev['TotalRevenue'] = 5000.0

    pred_low  = model.predict(low_rev[FEATURE_COLS])[0]
    pred_high = model.predict(high_rev[FEATURE_COLS])[0]
    assert pred_high > pred_low, \
        f'Test 2 FAILED: higher revenue should predict more, got {pred_high} vs {pred_low}'
    logger.info(
        f'Test 2 PASSED: low_rev={pred_low:.2f} < high_rev={pred_high:.2f}'
    )

    logger.info('All unit tests PASSED')


def main():
    """Main training pipeline."""
    logger.info('='*60)
    logger.info('ML TRAINING PIPELINE STARTING')
    logger.info(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info('='*60)

    # Setup MLflow
    mlflow.set_tracking_uri('file:///home/jovyan/work/mlruns')
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Load and prepare data
    df = load_features()
    df, le_stock, le_country = engineer_features(df)

    df_clean = df[FEATURE_COLS + [TARGET]].dropna()
    X = df_clean[FEATURE_COLS]
    y = df_clean[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    logger.info(f'Train: {len(X_train):,} | Test: {len(X_test):,}')

    # Train models
    models = [
        (LinearRegression(),                               'LinearRegression'),
        (RandomForestRegressor(n_estimators=100, n_jobs=-1), 'RandomForest'),
        (GradientBoostingRegressor(n_estimators=100),      'GradientBoosting'),
    ]

    results = []
    for model, name in models:
        trained_model, r2, rmse = train_model(
            model, name, X_train, X_test, y_train, y_test
        )
        results.append((trained_model, name, r2, rmse))

    # Select best model
    best = max(results, key=lambda x: x[2])
    best_model, best_name, best_r2, best_rmse = best
    logger.info(f'Best model: {best_name} (R2={best_r2:.4f})')

    # Run unit tests
    run_unit_tests(best_model)

    # Save model bundle
    save_model_bundle(best_model, best_name, le_stock, le_country)

    logger.info('='*60)
    logger.info('TRAINING COMPLETE')
    logger.info(f'Best Model : {best_name}')
    logger.info(f'R2 Score  : {best_r2:.4f}')
    logger.info(f'RMSE      : {best_rmse:.2f}')
    logger.info('='*60)


if __name__ == '__main__':
    main()
