import boto3
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

    items = None

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