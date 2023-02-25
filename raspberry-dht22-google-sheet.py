import time
import board
import adafruit_dht
from googleapiclient.discovery import build
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = {'https://www.googleapis.com/auth/spreadsheets'}

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

SPREADSHEET_ID = '<SPREADSHEET_ID>'

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
dhtDevice = adafruit_dht.DHT22(board.D22)

while True:
    try:
        temperature_c = dhtDevice.temperature
        humidity = dhtDevice.humidity
        local_date = time.localtime()
        date_str = time.strftime("%Y-%m-%d", local_date)
        time_str = time.strftime("%H:%M:%S", local_date)
        data = [[date_str, time_str, temperature_c, humidity]]
        request = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range="DATA!A1", valueInputOption="USER_ENTERED", body={"values":data}).execute()
    
    except (RuntimeError, Exception) as error:
        time.sleep(3.0)
        continue

    time.sleep(600.0)