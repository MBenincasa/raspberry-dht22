import boto3
from boto3.dynamodb.conditions import Key
import json

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

def calculate_differences(penultimate_data, last_data):
    differences = {
        "temperature_city": last_data["temperature_city"] - penultimate_data["temperature_city"],
        "humidity_sensor": last_data["humidity_sensor"] - penultimate_data["humidity_sensor"],
        "temperature_sensor_c": last_data["temperature_sensor_c"] - penultimate_data["temperature_sensor_c"],
        "humidity_city": last_data["humidity_city"] - penultimate_data["humidity_city"],
        "date": penultimate_data["date"],
        "time": penultimate_data["time"]
    }
    return differences

def lambda_handler(event, context):
    config_file_path = 'config.json'
    config_data = load_config(config_file_path)
    table = connect_to_dynamodb(config_data)
    
    filter_date = event.get('event_item', None)
    items = query_dynamodb(table, filter_date)

    if len(items) > 13:
        last_data = items[-1]
        penultimate_data = items[-13]
        differences = calculate_differences(penultimate_data, last_data)
        response = {"last_data": items[-1], "differences": differences}
    elif len(items) >= 2 and len(items) <= 13:
        last_data = items[-1]
        penultimate_data = items[0]
        differences = calculate_differences(penultimate_data, last_data)
        response = {"last_data": items[-1], "differences": differences}
    elif len(items) == 1:
        response = {"last_data": items[-1], "differences": None}
    else:
        response = {"last_data": None, "differences": None}

    return {
        'statusCode': 200,
        'body': response
    }
