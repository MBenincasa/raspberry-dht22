import json
import boto3

AWS_REGION = '<AWS_REGION>'
DYNAMODB_TABLE_NAME = '<DYNAMODB_TABLE_NAME>'
AWS_ACCESS_KEY_ID = '<AWS_ACCESS_KEY_ID>'
AWS_SECRET_ACCESS_KEY = '<AWS_SECRET_ACCESS_KEY>'

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

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
