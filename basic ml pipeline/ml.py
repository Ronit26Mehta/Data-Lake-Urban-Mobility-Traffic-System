import os
import requests
import random
import uuid
import csv
import pandas as pd
from flask import Flask, jsonify, request
from faker import Faker
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, accuracy_score

import os
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mehta\\Downloads\\main bde\\venv\\Scripts\\python.exe"
# Flask setup
app = Flask(__name__)

# Directory to store CSV files
OUTPUT_DIR = "output_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Faker
faker = Faker()

# Initialize Spark session
spark = SparkSession.builder.appName("VehicleDataAnalysis").getOrCreate()

# Function to fetch data from the Flask API
def fetch_data_from_api(endpoint):
    response = requests.get(f"http://127.0.0.1:5000/{endpoint}")
    return response.json()

# Function to prepare data for machine learning
def prepare_data_for_ml():
    # Fetch data
    vehicle_data = []
    for _ in range(100):
        vehicle_data.append(fetch_data_from_api('vehicle_data'))  # Fetch from your API

    # Convert vehicle data to DataFrame
    df = pd.DataFrame(vehicle_data)

    # Select relevant features for training
    df['speed'] = df['speed'].astype(float)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['fuelType'] = df['fuelType'].astype('category').cat.codes  # Convert categorical to numeric

    # Creating a Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Feature Engineering: Assemble features into a vector
    assembler = VectorAssembler(inputCols=['speed', 'latitude', 'longitude', 'fuelType'], outputCol='features')
    feature_df = assembler.transform(spark_df)

    return feature_df

# Function to train and evaluate machine learning models
def train_and_evaluate_models(feature_df):
    # Split the data into training and testing sets
    train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=123)

    # Logistic Regression Model
    lr = LogisticRegression(labelCol="fuelType", featuresCol="features")
    lr_model = lr.fit(train_data)
    lr_predictions = lr_model.transform(test_data)
    
    # Random Forest Classifier
    rf = RandomForestClassifier(labelCol="fuelType", featuresCol="features", numTrees=10)
    rf_model = rf.fit(train_data)
    rf_predictions = rf_model.transform(test_data)

    # Evaluate Models
    evaluator = BinaryClassificationEvaluator(labelCol="fuelType", rawPredictionCol="prediction")
    lr_auc = evaluator.evaluate(lr_predictions)
    rf_auc = evaluator.evaluate(rf_predictions)

    # Print model statistics in the terminal
    print("Logistic Regression AUC: ", lr_auc)
    print("Random Forest AUC: ", rf_auc)

    # Confusion Matrix and Accuracy
    lr_preds = lr_predictions.select("fuelType", "prediction").toPandas()
    rf_preds = rf_predictions.select("fuelType", "prediction").toPandas()

    lr_accuracy = accuracy_score(lr_preds['fuelType'], lr_preds['prediction'])
    rf_accuracy = accuracy_score(rf_preds['fuelType'], rf_preds['prediction'])
    print("Logistic Regression Accuracy: ", lr_accuracy)
    print("Random Forest Accuracy: ", rf_accuracy)

    lr_cm = confusion_matrix(lr_preds['fuelType'], lr_preds['prediction'])
    rf_cm = confusion_matrix(rf_preds['fuelType'], rf_preds['prediction'])
    
    # Display confusion matrix
    sns.heatmap(lr_cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Diesel', 'Electric', 'Hybrid', 'Petrol'], yticklabels=['Diesel', 'Electric', 'Hybrid', 'Petrol'])
    plt.title("Logistic Regression Confusion Matrix")
    plt.show()

    sns.heatmap(rf_cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Diesel', 'Electric', 'Hybrid', 'Petrol'], yticklabels=['Diesel', 'Electric', 'Hybrid', 'Petrol'])
    plt.title("Random Forest Confusion Matrix")
    plt.show()

# Main function to run
if __name__ == '__main__':
    feature_df = prepare_data_for_ml()
    train_and_evaluate_models(feature_df)
    app.run(debug=True, port=5000)
