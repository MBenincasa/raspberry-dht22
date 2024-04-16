import requests
import time
import boto3
from decimal import Decimal
import json

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

def migration():
    try:
        response = requests.get('https://script.google.com/macros/s/AKfycbxKr_YuE1XkcLdwQ9FbdIDHIXV-mbIEFq4HY1nV0DWypm7ncolzphj1Wn-LnjHqHCkF/exec')
        response.raise_for_status()
        data_dict = response.json()
        data_array = data_dict.get("data", [])

        for data_point in data_array:
            date_str = data_point["date"]
            time_str = data_point["time"]
            temperature_sensor_c = data_point.get("temperature", None)
            humidity_sensor = data_point.get("humidity", None)
            temperature_city = data_point.get("temperature_pero", None)
            humidity_city = data_point.get("humidity_pero", None)
            weather_description = data_point.get("weather_description", None)

            temperature_city = Decimal(str(temperature_city)) if temperature_city != '' else None
            humidity_city = Decimal(str(humidity_city)) if humidity_city != '' else None

            datetime_str = f"{date_str} {time_str}"
            timestamp = time.mktime(time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S"))

            print(f"Raw Values: {date_str}, {time_str}, {temperature_sensor_c}, {humidity_sensor}, {temperature_city}, {humidity_city}, {weather_description}")

            item = {
                'timestamp': Decimal(str(timestamp)),
                'date': date_str,
                'time': time_str,
                'temperature_sensor_c': Decimal(str(temperature_sensor_c)) if temperature_sensor_c is not None else None,
                'humidity_sensor': Decimal(str(humidity_sensor)) if humidity_sensor is not None else None,
                'temperature_city': Decimal(str(temperature_city)) if temperature_city is not None else None,
                'humidity_city': Decimal(str(humidity_city)) if humidity_city is not None else None,
                'weather_description': weather_description
            }
            table.put_item(Item=item)
            print(f"datetime_str {datetime_str} \n")
    except (RuntimeError, Exception) as e:
        print(e)

if __name__ == "__main__":
    migration()
