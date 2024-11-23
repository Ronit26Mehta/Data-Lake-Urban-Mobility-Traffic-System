import os
import random
import uuid
import csv
from flask import Flask, jsonify, request
from faker import Faker
from datetime import datetime, timedelta

# Directory to store CSV files
OUTPUT_DIR = "output_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Faker
faker = Faker()

# Constants for vehicle movement simulation
INDIRA_COL = {"latitude": 37.0841, "longitude": 78.9997}
INDIRA_POINT = {"latitude": 8.0883, "longitude": 93.9992}
LATITUDE_INCREMENT = (INDIRA_POINT['latitude'] - INDIRA_COL['latitude']) / 100
LONGITUDE_INCREMENT = (INDIRA_POINT['longitude'] - INDIRA_COL['longitude']) / 100

start_time = datetime.now()
start_location = INDIRA_COL.copy()

# Flask app
app = Flask(__name__)

# Enable logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Function to write data to a CSV file
def write_to_csv(filename, data):
    filepath = os.path.join(OUTPUT_DIR, f"{filename}.csv")
    file_exists = os.path.isfile(filepath)
    with open(filepath, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

# Function to simulate vehicle movement
def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location

# Function to get the next timestamp
def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time.isoformat()

# Function to generate vehicle data
def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    directions = ['North', 'South', 'East', 'West', 'Northeast', 'Northwest', 'Southeast', 'Southwest']
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time(),
        'latitude': location['latitude'],
        'longitude': location['longitude'],
        'speed': round(random.uniform(10, 120), 2),  # realistic speed range
        'direction': random.choice(directions),
        'fuelType': random.choice(['Petrol', 'Diesel', 'Electric', 'Hybrid'])
    }

# Vehicle Data Endpoint
@app.route('/vehicle_data', methods=['GET'])
def get_vehicle_data():
    vehicle_id = request.args.get('vehicle_id', 'Vehicle-Demo-123')
    logging.debug(f"Generating data for vehicle: {vehicle_id}")
    data = generate_vehicle_data(vehicle_id)
    write_to_csv('vehicle_data', data)
    return jsonify(data)

# Traffic Data Endpoint
@app.route('/traffic_data', methods=['GET'])
def get_traffic_data():
    data = {
        'camera_id': str(uuid.uuid4()),
        'timestamp': get_next_time(),
        'vehicle_count': random.randint(0, 200),
        'latitude': random.uniform(8.0883, 37.0841),
        'longitude': random.uniform(78.9997, 93.9992)
    }
    write_to_csv('traffic_data', data)
    return jsonify(data)

# Weather Data Endpoint
@app.route('/weather_data', methods=['GET'])
def get_weather_data():
    data = {
        'timestamp': get_next_time(),
        'temperature': round(random.uniform(-10, 45), 2),
        'humidity': random.randint(20, 90),
        'wind_speed': round(random.uniform(0, 15), 2),
        'weather_condition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Stormy'])
    }
    write_to_csv('weather_data', data)
    return jsonify(data)

# GPS Data Endpoint
@app.route('/gps_data', methods=['GET'])
def get_gps_data():
    data = {
        'vehicle_id': str(uuid.uuid4()),
        'timestamp': get_next_time(),
        'latitude': random.uniform(8.0883, 37.0841),
        'longitude': random.uniform(78.9997, 93.9992),
        'speed': round(random.uniform(0, 120), 2)
    }
    write_to_csv('gps_data', data)
    return jsonify(data)

# Emergency Data Endpoint
@app.route('/emergency_data', methods=['GET'])
def get_emergency_data():
    data = {
        'incident_id': str(uuid.uuid4()),
        'timestamp': get_next_time(),
        'type': random.choice(['Accident', 'Breakdown', 'Fire']),
        'latitude': random.uniform(8.0883, 37.0841),
        'longitude': random.uniform(78.9997, 93.9992),
        'status': random.choice(['Resolved', 'In Progress', 'Reported'])
    }
    write_to_csv('emergency_data', data)
    return jsonify(data)

# Health Check Endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "UP", "message": "The server is running fine!"})

# Main function to run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=5000)
