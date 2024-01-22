import boto3
from boto3.dynamodb.conditions import Key
import json
from collections import defaultdict

def lambda_handler(event, context):
    config_file_path = 'config.json'
    with open(config_file_path, 'r') as config_file:
        config_data = json.load(config_file)
    
    aws_region = config_data['aws_region']
    dynamodb_table_name = config_data['dynamodb_table_name']
    aws_access_key_id = config_data['aws_access_key_id']
    aws_secret_access_key = config_data['aws_secret_access_key']
    
    dynamodb = boto3.resource('dynamodb', region_name=aws_region, 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    table = dynamodb.Table(dynamodb_table_name)

    filter_date = event.get('filter_date', None)
    items = None

    if filter_date:
        response = table.query(
            KeyConditionExpression=Key('date').eq(filter_date)
        )
        items = response['Items']
        
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
    
        result_list = sorted(result_list, key=lambda x: x['timestamp'])
        return {
            'statusCode': 200,
            'body': result_list
        }
    else:
        response = table.scan()
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
            
        aggregated_data = defaultdict(lambda: {"temperature_sum": 0, "humidity_sum": 0, "count": 0})
        for item in items:
            aggregated_data[item.get('date')]["temperature_sum"] += item.get('temperature_sensor_c')
            aggregated_data[item.get('date')]["humidity_sum"] += item.get('humidity_sensor')
            aggregated_data[item.get('date')]["count"] += 1
            
        result_list = [{"date": date, "temperature_avg": data["temperature_sum"] / data["count"], "humidity_avg": data["humidity_sum"] / data["count"]} for date, data in aggregated_data.items()]
        result_list = sorted(result_list, key=lambda x: x['date'], reverse=True)
        return {
            'statusCode': 200,
            'body': result_list
        }
