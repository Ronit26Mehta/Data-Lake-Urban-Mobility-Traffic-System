import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pandas as pd
import time

os.environ["PYSPARK_PYTHON"] = "C:\\Users\\mehta\\Downloads\\main bde\\venv\\Scripts\\python.exe"

# Initialize Spark session
spark = SparkSession.builder.appName("VehicleDataAnalysis").getOrCreate()

# Function to fetch data from your API and convert it to a Spark DataFrame
def fetch_and_process_data(endpoint, params=None):
    response = requests.get(f"http://127.0.0.1:5000/{endpoint}", params=params)
    data = response.json()

    # Convert data to a Spark DataFrame
    df = spark.createDataFrame([data])
    return df

# Plotting function that will be updated continuously
def update_plots(vehicle_data_pandas, traffic_data_pandas, weather_data_pandas, gps_data_pandas, emergency_data_pandas):
    fig, axes = plt.subplots(3, 2, figsize=(15, 10))

    # Plotting average speed by direction
    avg_speed_by_direction = vehicle_data_pandas.groupby("direction")["speed"].mean().reset_index()
    sns.barplot(data=avg_speed_by_direction, x="direction", y="speed", ax=axes[0, 0], palette="Blues")
    axes[0, 0].set_title("Average Speed by Direction")
    axes[0, 0].set_xlabel("Direction")
    axes[0, 0].set_ylabel("Average Speed (km/h)")

    # Plotting average vehicle count by camera ID
    avg_vehicle_count_by_camera = traffic_data_pandas.groupby("camera_id")["vehicle_count"].mean().reset_index()
    sns.barplot(data=avg_vehicle_count_by_camera, x="camera_id", y="vehicle_count", ax=axes[0, 1], palette="Oranges")
    axes[0, 1].set_title("Average Vehicle Count by Camera ID")
    axes[0, 1].set_xlabel("Camera ID")
    axes[0, 1].set_ylabel("Average Vehicle Count")

    # Plotting temperature vs humidity
    sns.scatterplot(data=weather_data_pandas, x="temperature", y="humidity", ax=axes[1, 0], color="green")
    axes[1, 0].set_title("Temperature vs Humidity")
    axes[1, 0].set_xlabel("Temperature (°C)")
    axes[1, 0].set_ylabel("Humidity (%)")

    # Plotting GPS coordinates (latitude vs longitude)
    sns.scatterplot(data=gps_data_pandas, x="longitude", y="latitude", ax=axes[1, 1], color="purple")
    axes[1, 1].set_title("GPS Coordinates")
    axes[1, 1].set_xlabel("Longitude")
    axes[1, 1].set_ylabel("Latitude")

    # Plotting emergency incident status
    sns.countplot(data=emergency_data_pandas, x="status", ax=axes[2, 0], palette="Reds")
    axes[2, 0].set_title("Emergency Incident Status")
    axes[2, 0].set_xlabel("Status")
    axes[2, 0].set_ylabel("Count")

    # Hide the last subplot (not used)
    axes[2, 1].axis('off')

    plt.tight_layout()
    plt.draw()
    plt.pause(1)  # Pause to allow updates

# Function to fetch and process data and update plots
def fetch_and_update_data():
    while True:
        # Fetch and process vehicle data
        vehicle_data_df = fetch_and_process_data("vehicle_data")
        vehicle_data_df = vehicle_data_df.withColumn("speed", col("speed").cast("float"))

        # Fetch and process traffic data
        traffic_data_df = fetch_and_process_data("traffic_data")
        traffic_data_df = traffic_data_df.withColumn("vehicle_count", col("vehicle_count").cast("int"))

        # Fetch and process weather data
        weather_data_df = fetch_and_process_data("weather_data")

        # Fetch and process GPS data
        gps_data_df = fetch_and_process_data("gps_data")

        # Fetch and process emergency data
        emergency_data_df = fetch_and_process_data("emergency_data")

        # Convert Spark DataFrames to Pandas DataFrames for visualization
        vehicle_data_pandas = vehicle_data_df.toPandas()
        traffic_data_pandas = traffic_data_df.toPandas()
        weather_data_pandas = weather_data_df.toPandas()
        gps_data_pandas = gps_data_df.toPandas()
        emergency_data_pandas = emergency_data_df.toPandas()

        # Update the plots
        update_plots(vehicle_data_pandas, traffic_data_pandas, weather_data_pandas, gps_data_pandas, emergency_data_pandas)

        # Wait for a specific interval before fetching new data
        time.sleep(5)  # Fetch data and update plots every 5 seconds

# Initialize and start the data fetching and plot updating process
if __name__ == "__main__":
    plt.ion()  # Turn on interactive mode to allow continuous updates
    fetch_and_update_data()
