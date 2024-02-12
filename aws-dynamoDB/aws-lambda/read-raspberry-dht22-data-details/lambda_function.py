import boto3
from boto3.dynamodb.conditions import Key
import json
import statistics

def load_config(config_file_path):
    with open(config_file_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

def connect_to_dynamodb(config_data):
    aws_region = config_data['aws_region']
    dynamodb_table_name = config_data['dynamodb_table_name']
    aws_access_key_id = config_data['aws_access_key_id']
    aws_secret_access_key = config_data['aws_secret_access_key']
    
    dynamodb = boto3.resource('dynamodb', region_name=aws_region, 
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    table = dynamodb.Table(dynamodb_table_name)
    return table

def query_dynamodb(table, filter_date):
    response_query = table.query(
        KeyConditionExpression=Key('date').eq(filter_date)
    )
    items = response_query['Items']
    return items

def calculate_metrics(items):
    temperatures = [float(item.get('temperature_sensor_c')) for item in items if item.get('temperature_sensor_c') is not None]
    temperatures_city = [float(item.get('temperature_city')) for item in items if item.get('temperature_city') is not None]
    humidities = [float(item.get('humidity_sensor')) for item in items if item.get('humidity_sensor') is not None]
    humidities_city = [float(item.get('humidity_city')) for item in items if item.get('humidity_city') is not None]

    temperature_metrics = {
        'sensor_min': min(temperatures) if temperatures else None,
        'sensor_avg': statistics.mean(temperatures) if temperatures else None,
        'sensor_max': max(temperatures) if temperatures else None,
        'city_min': min(temperatures_city) if temperatures_city else None,
        'city_avg': statistics.mean(temperatures_city) if temperatures_city else None,
        'city_max': max(temperatures_city) if temperatures_city else None
    }

    humidity_metrics = {
        'sensor_min': min(humidities) if humidities else None,
        'sensor_avg': statistics.mean(humidities) if humidities else None,
        'sensor_max': max(humidities) if humidities else None,
        'city_min': min(humidities_city) if humidities_city else None,
        'city_avg': statistics.mean(humidities_city) if humidities_city else None,
        'city_max': max(humidities_city) if humidities_city else None
    }

    return temperature_metrics, humidity_metrics

def filter_unique_pollution_data(items):
    unique_items = []
    pollution_set = set()
    
    for item in items:
        if all(key in item for key in ['co_city', 'o3_city', 'nh3_city', 'no_city', 'no2_city', 'o3_city', 'pm10_city', 'pm2_5_city', 'so2_city', 'aqi_city']):
            pollution_data = {
                key: value for key, value in item.items() if key in ['co_city', 'o3_city', 'nh3_city', 'no_city', 'no2_city', 'o3_city', 'pm10_city', 'pm2_5_city', 'so2_city', 'aqi_city']
            }
            pollution_data_tuple = tuple(sorted(pollution_data.items()))
        
            if pollution_data_tuple not in pollution_set:
                unique_items.append(item)
                pollution_set.add(pollution_data_tuple)
    
    return unique_items

def transform_pollution_items(items):
    result_list = []
    for item in items:
        pollution_data = {
            key: value for key, value in item.items() if key in ['time', 'date'] or key in ['co_city', 'o3_city', 'nh3_city', 'no_city', 'no2_city', 'o3_city', 'pm10_city', 'pm2_5_city', 'so2_city', 'aqi_city']
        }
        result_list.append(pollution_data)
    return result_list

def transform_items(items):
    result_list = []
    for item in items:
        timestamp = item.get('timestamp')
        date = item.get('date')
        time = item.get('time')
        temperature_sensor_c = item.get('temperature_sensor_c')
        temperature_city = item.get('temperature_city')
        humidity_sensor = item.get('humidity_sensor')
        humidity_city = item.get('humidity_city')
        weather_description = item.get('weather_description')

        result_list.append({
            'timestamp': float(timestamp),
            'date': date,
            'time': time,
            'temperature': float(temperature_sensor_c) if temperature_sensor_c is not None else None,
            'temperature_pero': float(temperature_city) if temperature_city is not None else None,
            'humidity': float(humidity_sensor) if humidity_sensor is not None else None,
            'humidity_pero': float(humidity_city) if humidity_city is not None else None,
            'weather_description': weather_description,
        })
    return result_list

def lambda_handler(event, context):
    config_file_path = 'config.json'
    config_data = load_config(config_file_path)
    table = connect_to_dynamodb(config_data)
    
    filter_date = event.get('event_item', None)
    items = query_dynamodb(table, filter_date)
    
    result_list = transform_items(items)
    pollution_items = filter_unique_pollution_data(items)
    pollution_list = transform_pollution_items(pollution_items)
    temperature_metrics, humidity_metrics = calculate_metrics(items)

    response = {
        'weather_data': result_list,
        'pollution_data': pollution_list,
        'metrics': {
            'temperature': temperature_metrics,
            'humidity': humidity_metrics
        }
    }
    
    return {
        'statusCode': 200,
        'body': response
    }
