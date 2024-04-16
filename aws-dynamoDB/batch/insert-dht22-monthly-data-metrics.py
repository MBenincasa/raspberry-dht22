import boto3
import json
from collections import defaultdict
from decimal import Decimal

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

def scan_dynamodb_table():
    items = []
    response = table_data.scan()
    items.extend(response.get('Items', []))
    while 'LastEvaluatedKey' in response:
        response = table_data.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    return items

def aggregate_data(items):
    aggregated_data = defaultdict(lambda: {
        "temperature_sensor_sum": 0, "humidity_sensor_sum": 0, 
        "temperature_city_sum": 0, "humidity_city_sum": 0, 
        "count_weather_sensor": 0, "count_weather_city": 0, 
        "pm2_5_city_sum": 0, "pm10_city_sum": 0, "no2_city_sum": 0, 
        "count_pollution_city": 0,
        "temperature_sensor_min": float('inf'), "temperature_sensor_max": float('-inf'),
        "humidity_sensor_min": float('inf'), "humidity_sensor_max": float('-inf'),
        "temperature_city_min": None, "temperature_city_max": None,
        "humidity_city_min": None, "humidity_city_max": None,
        "pm2_5_city_min": None, "pm2_5_city_max": None,
        "pm10_city_min": None, "pm10_city_max": None,
        "no2_city_min": None, "no2_city_max": None
    })

    for item in items:
        date = item.get('date')
        temperature_sensor = item.get('temperature_sensor_c')
        humidity_sensor = item.get('humidity_sensor')
        temperature_city = item.get('temperature_city')
        humidity_city = item.get('humidity_city')
        pm2_5_city = item.get('pm2_5_city')
        pm10_city = item.get('pm10_city')
        no2_city = item.get('no2_city')

        if all(v is not None for v in [date, temperature_sensor, humidity_sensor]):
            aggregated_data[date]["temperature_sensor_sum"] += temperature_sensor
            aggregated_data[date]["humidity_sensor_sum"] += humidity_sensor
            aggregated_data[date]["count_weather_sensor"] += 1
            aggregated_data[date]["temperature_sensor_min"] = min(aggregated_data[date]["temperature_sensor_min"], temperature_sensor)
            aggregated_data[date]["temperature_sensor_max"] = max(aggregated_data[date]["temperature_sensor_max"], temperature_sensor)
            aggregated_data[date]["humidity_sensor_min"] = min(aggregated_data[date]["humidity_sensor_min"], humidity_sensor)
            aggregated_data[date]["humidity_sensor_max"] = max(aggregated_data[date]["humidity_sensor_max"], humidity_sensor)

        if all(v is not None for v in [date, temperature_city, humidity_city]):
            aggregated_data[date]["temperature_city_sum"] += temperature_city
            aggregated_data[date]["humidity_city_sum"] += humidity_city
            aggregated_data[date]["count_weather_city"] += 1
            aggregated_data[date]["temperature_city_min"] = min(aggregated_data[date]["temperature_city_min"] or float('inf'), temperature_city)
            aggregated_data[date]["temperature_city_max"] = max(aggregated_data[date]["temperature_city_max"] or float('-inf'), temperature_city)
            aggregated_data[date]["humidity_city_min"] = min(aggregated_data[date]["humidity_city_min"] or float('inf'), humidity_city)
            aggregated_data[date]["humidity_city_max"] = max(aggregated_data[date]["humidity_city_max"] or float('-inf'), humidity_city)

        if all(v is not None for v in [date, pm2_5_city, pm10_city, no2_city]):
            aggregated_data[date]["pm2_5_city_sum"] += pm2_5_city
            aggregated_data[date]["pm10_city_sum"] += pm10_city
            aggregated_data[date]["no2_city_sum"] += no2_city
            aggregated_data[date]["count_pollution_city"] += 1
            aggregated_data[date]["pm2_5_city_min"] = min(aggregated_data[date]["pm2_5_city_min"] or float('inf'), pm2_5_city)
            aggregated_data[date]["pm2_5_city_max"] = max(aggregated_data[date]["pm2_5_city_max"] or float('-inf'), pm2_5_city)
            aggregated_data[date]["pm10_city_min"] = min(aggregated_data[date]["pm10_city_min"] or float('inf'), pm10_city)
            aggregated_data[date]["pm10_city_max"] = max(aggregated_data[date]["pm10_city_max"] or float('-inf'), pm10_city)
            aggregated_data[date]["no2_city_min"] = min(aggregated_data[date]["no2_city_min"] or float('inf'), no2_city)
            aggregated_data[date]["no2_city_max"] = max(aggregated_data[date]["no2_city_max"] or float('-inf'), no2_city)

    return aggregated_data

def calculate_metrics(aggregated_data):
    result_list = []
    for date, data in aggregated_data.items():
        temperature_sensor_avg = Decimal(round(data["temperature_sensor_sum"] / data["count_weather_sensor"], 2)) if data["count_weather_sensor"] > 0 else None
        humidity_sensor_avg = Decimal(round(data["humidity_sensor_sum"] / data["count_weather_sensor"], 2)) if data["count_weather_sensor"] > 0 else None
        temperature_city_avg = Decimal(round(data["temperature_city_sum"] / data["count_weather_city"], 2)) if data["count_weather_city"] > 0 else None
        humidity_city_avg = Decimal(round(data["humidity_city_sum"] / data["count_weather_city"], 2)) if data["count_weather_city"] > 0 else None
        pm2_5_city_avg = Decimal(round(data["pm2_5_city_sum"] / data["count_pollution_city"], 2)) if data["count_pollution_city"] > 0 else None
        pm10_city_avg = Decimal(round(data["pm10_city_sum"] / data["count_pollution_city"], 2)) if data["count_pollution_city"] > 0 else None
        no2_city_avg = Decimal(round(data["no2_city_sum"] / data["count_pollution_city"], 2)) if data["count_pollution_city"] > 0 else None
        
        result_list.append({
            "date": date,
            "temperature_sensor_avg": temperature_sensor_avg,
            "humidity_sensor_avg": humidity_sensor_avg,
            "temperature_city_avg": temperature_city_avg,
            "humidity_city_avg": humidity_city_avg,
            "pm2_5_city_avg": pm2_5_city_avg,
            "pm10_city_avg": pm10_city_avg,
            "no2_city_avg": no2_city_avg,
            "temperature_sensor_min": Decimal(data["temperature_sensor_min"]) if data["temperature_sensor_min"] is not None else None,
            "temperature_sensor_max": Decimal(data["temperature_sensor_max"]) if data["temperature_sensor_max"] is not None else None,
            "humidity_sensor_min": Decimal(data["humidity_sensor_min"]) if data["humidity_sensor_min"] is not None else None,
            "humidity_sensor_max": Decimal(data["humidity_sensor_max"]) if data["humidity_sensor_max"] is not None else None,
            "temperature_city_min": Decimal(data["temperature_city_min"]) if data["temperature_city_min"] is not None else None,
            "temperature_city_max": Decimal(data["temperature_city_max"]) if data["temperature_city_max"] is not None else None,
            "humidity_city_min": Decimal(data["humidity_city_min"]) if data["humidity_city_min"] is not None else None,
            "humidity_city_max": Decimal(data["humidity_city_max"]) if data["humidity_city_max"] is not None else None,
            "pm2_5_city_min": Decimal(data["pm2_5_city_min"]) if data["pm2_5_city_min"] is not None else None,
            "pm2_5_city_max": Decimal(data["pm2_5_city_max"]) if data["pm2_5_city_max"] is not None else None,
            "pm10_city_min": Decimal(data["pm10_city_min"]) if data["pm10_city_min"] is not None else None,
            "pm10_city_max": Decimal(data["pm10_city_max"]) if data["pm10_city_max"] is not None else None,
            "no2_city_min": Decimal(data["no2_city_min"]) if data["no2_city_min"] is not None else None,
            "no2_city_max": Decimal(data["no2_city_max"]) if data["no2_city_max"] is not None else None
        })
    return result_list

def write_into_dynamodb(grouped_results):
    for year_month in set(grouped_results.keys()):
        for item_metrics in grouped_results[year_month]:
            item_metrics['year_month'] = year_month
            print(f"{item_metrics}\n")
            table_metrics.put_item(Item=item_metrics)

def insert_data():
    items = scan_dynamodb_table()
    aggregated_data = aggregate_data(items)
    result_list = calculate_metrics(aggregated_data)

    grouped_results = defaultdict(list)
    for item in result_list:
        year_month = item['date'][:7]
        grouped_results[year_month].append(item)

    write_into_dynamodb(grouped_results)

if __name__ == "__main__":
    insert_data()
