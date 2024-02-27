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

def lambda_handler(event, context):
    config_file_path = 'config.json'
    config_data = load_config(config_file_path)
    table = connect_to_dynamodb(config_data)
    
    filter_date = event.get('event_item', None)
    items = query_dynamodb(table, filter_date)

    response = {"last_data": items[-1]} if items else {"last_data": None}

    return {
        'statusCode': 200,
        'body': response
    }
