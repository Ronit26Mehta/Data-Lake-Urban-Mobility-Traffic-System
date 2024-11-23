import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("VehicleDataAnalysis").getOrCreate()

# Function to fetch data from your API and convert it to a Spark DataFrame
def fetch_and_process_data(endpoint, params=None):
    response = requests.get(f"http://127.0.0.1:5000/{endpoint}", params=params)
    data = response.json()

    # Convert data to a Spark DataFrame
    df = spark.createDataFrame([data])

    return df

# Fetch and process vehicle data
vehicle_data_df = fetch_and_process_data("vehicle_data")

# Process vehicle data
vehicle_data_df = vehicle_data_df.withColumn("speed", col("speed").cast("float"))

# Show the data structure
vehicle_data_df.show()

# Calculate average speed by direction
avg_speed_by_direction = vehicle_data_df.groupBy("direction").avg("speed").collect()

# Convert results to a Pandas DataFrame for plotting
avg_speed_by_direction_pandas = {
    "direction": [row["direction"] for row in avg_speed_by_direction],
    "avg_speed": [row["avg(speed)"] for row in avg_speed_by_direction],
}

# Fetch and process traffic data
traffic_data_df = fetch_and_process_data("traffic_data")

# Process traffic data
traffic_data_df = traffic_data_df.withColumn("vehicle_count", col("vehicle_count").cast("int"))

# Show the data structure
traffic_data_df.show()

# Calculate average vehicle count by camera_id
avg_vehicle_count_by_camera = traffic_data_df.groupBy("camera_id").avg("vehicle_count").collect()

# Convert results to a Pandas DataFrame for plotting
avg_vehicle_count_by_camera_pandas = {
    "camera_id": [row["camera_id"] for row in avg_vehicle_count_by_camera],
    "avg_vehicle_count": [row["avg(vehicle_count)"] for row in avg_vehicle_count_by_camera],
}

# Step 3: Visualization
fig, ax = plt.subplots(1, 2, figsize=(10, 5))

# Plotting average speed by direction
ax[0].bar(avg_speed_by_direction_pandas["direction"], avg_speed_by_direction_pandas["avg_speed"], color='blue')
ax[0].set_title("Average Speed by Direction")
ax[0].set_xlabel("Direction")
ax[0].set_ylabel("Average Speed (km/h)")

# Plotting average vehicle count by camera ID
ax[1].bar(avg_vehicle_count_by_camera_pandas["camera_id"], avg_vehicle_count_by_camera_pandas["avg_vehicle_count"], color='orange')
ax[1].set_title("Average Vehicle Count by Camera ID")
ax[1].set_xlabel("Camera ID")
ax[1].set_ylabel("Average Vehicle Count")

plt.tight_layout()
plt.show()
