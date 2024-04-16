import boto3
import json
from collections import defaultdict
from datetime import datetime

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
    
    response = {
        "statusCode": 200,
        "body": {
            "data": []
        }
    }
    
    data_dict = {}
    for item in items:
        year_month = item['year_month']
        if year_month not in data_dict:
            data_dict[year_month] = []
        data_dict[year_month].append(item)
    
    sorted_year_months = sorted(data_dict.keys())
    for year_month in sorted_year_months:
        data_entry = {
            "date": year_month,
            "weather_aggregation": [],
            "pollution_aggregation": []
        }
        
        for item in data_dict[year_month]:
            weather_entry = {
                "date": item['date'],
                "temperature_sensor_avg": item['temperature_sensor_avg'],
                "humidity_sensor_avg": item['humidity_sensor_avg'],
                "temperature_city_avg": item['temperature_city_avg'],
                "humidity_city_avg": item['humidity_city_avg']
            }
            data_entry["weather_aggregation"].append(weather_entry)
            
            if all(item[city_avg] is not None for city_avg in ['pm2_5_city_avg', 'pm10_city_avg', 'no2_city_avg']):
                pollution_entry = {
                    "date": item['date'],
                    "pm2_5_city_avg": item['pm2_5_city_avg'],
                    "pm10_city_avg": item['pm10_city_avg'],
                    "no2_city_avg": item['no2_city_avg']
                }
                data_entry["pollution_aggregation"].append(pollution_entry)
        
        if not data_entry["pollution_aggregation"]:
            data_entry["pollution_aggregation"] = None
        
        response["body"]["data"].append(data_entry)
    
    return response
