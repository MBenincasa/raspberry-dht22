import boto3
import json

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
    filter_expression = None
    expression_attribute_values = None

    if filter_date:
        filter_expression = "#date_attr = :date_val"
        expression_attribute_values = {":date_val": filter_date}
    
        response = table.scan(
            FilterExpression=filter_expression,
            ExpressionAttributeNames={"#date_attr": "date"},
            ExpressionAttributeValues=expression_attribute_values
        )
    else:
        response = table.scan()
    items = response.get('Items', [])
    
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
            'temperature': float(temperature_sensor_c),
            'temperature_pero': float(temperature_city),
            'humidity': float(humidity_sensor),
            'humidity_pero': float(humidity_city),
            'weather_description': weather_description,
        })
    
    return {
        'statusCode': 200,
        'body': result_list
    }
