import time
import board
import adafruit_dht
import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = {'https://www.googleapis.com/auth/spreadsheets'}

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

SPREADSHEET_ID = '<SPREADSHEET_ID>'
OPENWEATHERMAP_API_KEY = '<OPENWEATHERMAP_API_KEY>'

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
dhtDevice = adafruit_dht.DHT22(board.D22)

while True:
    try:
        # Reading from DHT22 sensor
        temperature_sensor_c = dhtDevice.temperature
        humidity_sensor = dhtDevice.humidity
        
        # Reading from OpenWeatherMap
        openweathermap_url = f'https://api.openweathermap.org/data/2.5/weather?lat=45.5101617&lon=9.0894415&appid={OPENWEATHERMAP_API_KEY}&units=metric'
        response_openweathermap = requests.get(openweathermap_url)
        weather_data = response_openweathermap.json()
        temperature_city = weather_data['main']['temp']
        humidity_city = weather_data['main']['humidity']
        weather_description = weather_data['weather'][0]['description']

        # Reading local date time
        local_date = time.localtime()
        date_str = time.strftime("%Y-%m-%d", local_date)
        time_str = time.strftime("%H:%M:%S", local_date)
        
        data = [[date_str, time_str, temperature_sensor_c, humidity_sensor, temperature_city, humidity_city, weather_description]]
        request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range="DATA!A1", valueInputOption="USER_ENTERED", body={"values":data}).execute()
    
    except (RuntimeError, Exception) as error:
        time.sleep(3.0)
        continue

    time.sleep(600.0)