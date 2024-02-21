import boto3
import json
from collections import defaultdict

def get_config():
    config_file_path = 'config.json'
    with open(config_file_path, 'r') as config_file:
        return json.load(config_file)

def scan_dynamodb_table(table):
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    return items

def aggregate_data(items):
    aggregated_data = defaultdict(lambda: {"temperature_sensor_sum": 0, "humidity_sensor_sum": 0, "temperature_city_sum": 0, "humidity_city_sum": 0, "count": 0})
    for item in items:
        date = item.get('date')
        temperature_sensor = item.get('temperature_sensor_c')
        humidity_sensor = item.get('humidity_sensor')
        temperature_city = item.get('temperature_city')
        humidity_city = item.get('humidity_city')
        
        if all(v is not None for v in [date, temperature_sensor, humidity_sensor, temperature_city, humidity_city]):
            aggregated_data[date]["temperature_sensor_sum"] += temperature_sensor
            aggregated_data[date]["humidity_sensor_sum"] += humidity_sensor
            aggregated_data[date]["temperature_city_sum"] += temperature_city
            aggregated_data[date]["humidity_city_sum"] += humidity_city
            aggregated_data[date]["count"] += 1
    return aggregated_data

def calculate_averages(aggregated_data):
    result_list = []
    for date, data in aggregated_data.items():
        temperature_sensor_avg = round(data["temperature_sensor_sum"] / data["count"], 2) if data["count"] > 0 else None
        humidity_sensor_avg = round(data["humidity_sensor_sum"] / data["count"], 2) if data["count"] > 0 else None
        temperature_city_avg = round(data["temperature_city_sum"] / data["count"], 2) if data["count"] > 0 else None
        humidity_city_avg = round(data["humidity_city_sum"] / data["count"], 2) if data["count"] > 0 else None
        
        result_list.append({
            "date": date,
            "temperature_sensor_avg": temperature_sensor_avg,
            "humidity_sensor_avg": humidity_sensor_avg,
            "temperature_city_avg": temperature_city_avg,
            "humidity_city_avg": humidity_city_avg
        })
    return result_list

def lambda_handler(event, context):
    config_data = get_config()
    aws_region = config_data['aws_region']
    dynamodb_table_name = config_data['dynamodb_table_name']
    aws_access_key_id = config_data['aws_access_key_id']
    aws_secret_access_key = config_data['aws_secret_access_key']
    
    dynamodb = boto3.resource('dynamodb', region_name=aws_region, 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    table = dynamodb.Table(dynamodb_table_name)

    items = scan_dynamodb_table(table)
    aggregated_data = aggregate_data(items)
    result_list = calculate_averages(aggregated_data)
    
    result_list = sorted(result_list, key=lambda x: x['date'])
    response = {
        "data": result_list
    }
    return {
        'statusCode': 200,
        'body': response
    }
