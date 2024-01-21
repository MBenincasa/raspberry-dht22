import time
import board
import adafruit_dht
import requests
import boto3
from decimal import Decimal
import json
import hashlib

def read_config():
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

config = read_config()

AWS_REGION = config['aws_region']
DYNAMODB_TABLE_NAME = config['dynamodb_table_name']
AWS_ACCESS_KEY_ID = config['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws_secret_access_key']
OPENWEATHERMAP_API_KEY = config['openweathermap_api_key']

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

dhtDevice = adafruit_dht.DHT22(board.D22)

def read_dht22_sensor():
    try:
        # Reading from DHT22 sensor
        temperature_sensor_c = dhtDevice.temperature
        humidity_sensor = dhtDevice.humidity
        return temperature_sensor_c, humidity_sensor
    except RuntimeError as error:
        print(f"Error reading DHT22 sensor: {error}")
        raise

def read_openweathermap_data():
    try:
        # Reading from OpenWeatherMap
        openweathermap_url = f'https://api.openweathermap.org/data/2.5/weather?lat=45.5101617&lon=9.0894415&appid={OPENWEATHERMAP_API_KEY}&units=metric'
        response_openweathermap = requests.get(openweathermap_url)
        weather_data = response_openweathermap.json()
        temperature_city = weather_data['main']['temp']
        humidity_city = weather_data['main']['humidity']
        weather_description = weather_data['weather'][0]['description']
        return temperature_city, humidity_city, weather_description
    except Exception as error:
        print(f"Error reading OpenWeatherMap data: {error}")
        raise

def get_local_datetime():
    # Reading local date time
    local_date = time.localtime()
    date_str = time.strftime("%Y-%m-%d", local_date)
    time_str = time.strftime("%H:%M:%S", local_date)
    datetime_str = f"{date_str} {time_str}"
    timestamp = time.mktime(time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S"))
    id = hashlib.md5(datetime_str.encode()).hexdigest()
    return id, timestamp, date_str, time_str

def write_to_dynamodb(id, timestamp, date_str, time_str, temperature_sensor_c, humidity_sensor, temperature_city, humidity_city, weather_description):
    item = {
        'id': id,
        'timestamp': Decimal(str(timestamp)),
        'date': date_str,
        'time': time_str,
        'temperature_sensor_c': Decimal(str(temperature_sensor_c)),
        'humidity_sensor': Decimal(str(humidity_sensor)),
        'temperature_city': Decimal(str(temperature_city)),
        'humidity_city': Decimal(str(humidity_city)),
        'weather_description': weather_description
    }
    table.put_item(Item=item)

if __name__ == "__main__":
    while True:
        try:
            temperature_sensor_c, humidity_sensor = read_dht22_sensor()
            temperature_city, humidity_city, weather_description = read_openweathermap_data()
            id, timestamp, date_str, time_str = get_local_datetime()

            write_to_dynamodb(id, timestamp, date_str, time_str, temperature_sensor_c, humidity_sensor, temperature_city, humidity_city, weather_description)
        except (RuntimeError, Exception) as error:
            time.sleep(5.0)
            continue

        time.sleep(600.0)