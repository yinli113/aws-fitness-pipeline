import argparse
import os

# Third-party imports (e.g., pandas, scikit-learn, boto3)
# These would need to be installed in your execution environment (Glue/SageMaker)
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_squared_error
import joblib # For saving models
import boto3

def load_data_from_silver(silver_bucket, prefix=""):
    """
    Loads data from the Silver S3 bucket into a pandas DataFrame.
    NOTE: This is a simplified example. For large datasets, use Spark on Glue/SageMaker.
    """
    # This is a placeholder implementation.
    # In a real scenario, you would use s3fs or boto3 to read Parquet files.
    print(f"Loading data from s3://{silver_bucket}/{prefix}...")
    # Example:
    # df = pd.read_parquet(f"s3://{silver_bucket}/{prefix}")
    # return df
    
    # Returning a dummy dataframe for demonstration purposes
    data = {
        'user_id': [1, 1, 2, 2],
        'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03']),
        'heart_rate_7day_avg': [75, 76, 80, 81],
        'steps_lag_1': [5000, 5200, 4500, 4600],
        'is_stressed': [0, 1, 0, 1], # Target for high-stress classifier
        'sleep_hours': [7.5, 6.0, 8.0, 7.0], # Target for sleep-hours regressor
        'weight_kg': [70, 70.1, 85, 84.9] # Target for weight regressor
    }
    return pd.DataFrame(data)


def train_stress_classifier(df):
    """Trains a classifier to predict high-stress days."""
    print("Training high-stress classifier...")
    features = ['heart_rate_7day_avg', 'steps_lag_1']
    target = 'is_stressed'
    
    X_train, X_test, y_train, y_test = train_test_split(df[features], df[target], test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate model
    preds = model.predict(X_test)
    print(f"Stress classifier accuracy: {accuracy_score(y_test, preds):.2f}")
    return model

def train_sleep_regressor(df):
    """Trains a regressor to predict sleep hours."""
    print("Training sleep-hours regressor...")
    features = ['heart_rate_7day_avg', 'steps_lag_1']
    target = 'sleep_hours'
    
    X_train, X_test, y_train, y_test = train_test_split(df[features], df[target], test_size=0.2, random_state=42)
    
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate model
    preds = model.predict(X_test)
    print(f"Sleep regressor RMSE: {mean_squared_error(y_test, preds, squared=False):.2f}")
    return model

def train_weight_regressor(df):
    """Trains a regressor to predict weight."""
    print("Training weight regressor...")
    features = ['heart_rate_7day_avg', 'steps_lag_1']
    target = 'weight_kg'
    
    X_train, X_test, y_train, y_test = train_test_split(df[features], df[target], test_size=0.2, random_state=42)
    
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate model
    preds = model.predict(X_test)
    print(f"Weight regressor RMSE: {mean_squared_error(y_test, preds, squared=False):.2f}")
    return model

def save_predictions(df, models, gold_bucket, output_prefix):
    """
    Generates predictions using trained models and saves them to the Gold S3 bucket.
    """
    print("Generating and saving predictions...")
    predictions_df = df.copy()
    predictions_df['stress_prediction'] = models['stress'].predict(df[['heart_rate_7day_avg', 'steps_lag_1']])
    predictions_df['sleep_prediction'] = models['sleep'].predict(df[['heart_rate_7day_avg', 'steps_lag_1']])
    predictions_df['weight_prediction'] = models['weight'].predict(df[['heart_rate_7day_avg', 'steps_lag_1']])
    
    output_path = f"s3://{gold_bucket}/{output_prefix}/"
    print(f"Saving predictions to {output_path}")
    # In a real scenario, you'd write this DataFrame to S3 as a Parquet file.
    # predictions_df.to_parquet(output_path)
    print("Predictions (sample):\n", predictions_df.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-bucket", type=str, required=True, help="S3 bucket for Silver data")
    parser.add_argument("--gold-bucket", type=str, required=True, help="S3 bucket for Gold data")
    args = parser.parse_args()

    # 1. Load data
    fitness_data = load_data_from_silver(args.silver_bucket)

    # 2. Train models
    stress_model = train_stress_classifier(fitness_data)
    sleep_model = train_sleep_regressor(fitness_data)
    weight_model = train_weight_regressor(fitness_data)

    # 3. Save predictions
    trained_models = {
        'stress': stress_model,
        'sleep': sleep_model,
        'weight': weight_model,
    }
    save_predictions(fitness_data, trained_models, args.gold_bucket, "fact_predictions")
