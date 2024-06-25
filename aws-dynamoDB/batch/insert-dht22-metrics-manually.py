import boto3
import json
from decimal import Decimal
from statistics import mean

from boto3.dynamodb.conditions import Key

def read_config():
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

config = read_config()

AWS_REGION = config['aws_region']
DYNAMODB_TABLE_NAME = config['dynamodb_table_name']
DYNAMODB_TABLE_NAME_MONTHLY_METRICS = config['dynamodb_table_name_monthly_metrics']
AWS_ACCESS_KEY_ID = config['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws_secret_access_key']
OPENWEATHERMAP_API_KEY = config['openweathermap_api_key']

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

table_data = dynamodb.Table(DYNAMODB_TABLE_NAME)
table_metrics = dynamodb.Table(DYNAMODB_TABLE_NAME_MONTHLY_METRICS)

def query_dynamodb(yesterday_str):
    response_query = table_data.query(
        KeyConditionExpression=Key('date').eq(yesterday_str)
    )
    items = response_query['Items']
    return items

def calculate_metrics(items):
    temperature_sensor_values = []
    humidity_sensor_values = []
    temperature_city_values = []
    humidity_city_values = []
    pm2_5_city_values = []
    pm10_city_values = []
    no2_city_values = []

    for item in items:
        temperature_sensor_values.append(float(item['temperature_sensor_c']))
        humidity_sensor_values.append(float(item['humidity_sensor']))
        temperature_city_values.append(float(item['temperature_city']))
        humidity_city_values.append(float(item['humidity_city']))
        pm2_5_city_values.append(float(item['pm2_5_city']))
        pm10_city_values.append(float(item['pm10_city']))
        no2_city_values.append(float(item['no2_city']))

    temperature_sensor_avg = mean(temperature_sensor_values)
    humidity_sensor_avg = mean(humidity_sensor_values)
    temperature_city_avg = mean(temperature_city_values)
    humidity_city_avg = mean(humidity_city_values)
    pm2_5_city_avg = mean(pm2_5_city_values)
    pm10_city_avg = mean(pm10_city_values)
    no2_city_avg = mean(no2_city_values)

    temperature_sensor_min = min(temperature_sensor_values) if temperature_sensor_values else None
    temperature_sensor_max = max(temperature_sensor_values) if temperature_sensor_values else None
    humidity_sensor_min = min(humidity_sensor_values) if humidity_sensor_values else None
    humidity_sensor_max = max(humidity_sensor_values) if humidity_sensor_values else None
    temperature_city_min = min(temperature_city_values) if temperature_city_values else None
    temperature_city_max = max(temperature_city_values) if temperature_city_values else None
    humidity_city_min = min(humidity_city_values) if humidity_city_values else None
    humidity_city_max = max(humidity_city_values) if humidity_city_values else None
    pm2_5_city_min = min(pm2_5_city_values) if pm2_5_city_values else None
    pm2_5_city_max = max(pm2_5_city_values) if pm2_5_city_values else None
    pm10_city_min = min(pm10_city_values) if pm10_city_values else None
    pm10_city_max = max(pm10_city_values) if pm10_city_values else None
    no2_city_min = min(no2_city_values) if no2_city_values else None
    no2_city_max = max(no2_city_values) if no2_city_values else None

    date = items[0]['date']
    year_month = date[:7]

    quantize_decimal = lambda x: Decimal(str(x)).quantize(Decimal('0.00'))

    metrics = {
        "date": date,
        "year_month": year_month,
        "temperature_sensor_avg": quantize_decimal(temperature_sensor_avg),
        "humidity_sensor_avg": quantize_decimal(humidity_sensor_avg),
        "temperature_city_avg": quantize_decimal(temperature_city_avg),
        "humidity_city_avg": quantize_decimal(humidity_city_avg),
        "pm2_5_city_avg": quantize_decimal(pm2_5_city_avg),
        "pm10_city_avg": quantize_decimal(pm10_city_avg),
        "no2_city_avg": quantize_decimal(no2_city_avg),
        "temperature_sensor_min": quantize_decimal(temperature_sensor_min) if temperature_sensor_min is not None else None,
        "temperature_sensor_max": quantize_decimal(temperature_sensor_max) if temperature_sensor_max is not None else None,
        "humidity_sensor_min": quantize_decimal(humidity_sensor_min) if humidity_sensor_min is not None else None,
        "humidity_sensor_max": quantize_decimal(humidity_sensor_max) if humidity_sensor_max is not None else None,
        "temperature_city_min": quantize_decimal(temperature_city_min) if temperature_city_min is not None else None,
        "temperature_city_max": quantize_decimal(temperature_city_max) if temperature_city_max is not None else None,
        "humidity_city_min": quantize_decimal(humidity_city_min) if humidity_city_min is not None else None,
        "humidity_city_max": quantize_decimal(humidity_city_max) if humidity_city_max is not None else None,
        "pm2_5_city_min": quantize_decimal(pm2_5_city_min) if pm2_5_city_min is not None else None,
        "pm2_5_city_max": quantize_decimal(pm2_5_city_max) if pm2_5_city_max is not None else None,
        "pm10_city_min": quantize_decimal(pm10_city_min) if pm10_city_min is not None else None,
        "pm10_city_max": quantize_decimal(pm10_city_max) if pm10_city_max is not None else None,
        "no2_city_min": quantize_decimal(no2_city_min) if no2_city_min is not None else None,
        "no2_city_max": quantize_decimal(no2_city_max) if no2_city_max is not None else None
    }
    return metrics

def insert_daily_metrics(date):
    items = query_dynamodb(date)

    metrics = calculate_metrics(items)
    print(f"{metrics}")

    table_metrics.put_item(Item=metrics)

def main():
    insert_daily_metrics("2024-06-19")

if __name__ == "__main__":
    main()
