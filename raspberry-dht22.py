import time
import board
import adafruit_dht
import csv
import os

dhtDevice = adafruit_dht.DHT22(board.D22)

while True:
    try:
        temperature_c = dhtDevice.temperature
        humidity = dhtDevice.humidity
        local_date = time.localtime()
        local_date_str = time.strftime("%Y-%m-%d %H:%M:%S", local_date)

        filename = time.strftime("dht22_data_%Y-%m-%d.csv")
        exists = os.path.exists(filename)
        with open(filename, 'a') as csvfile:
            fieldnames = ['date_time', 'temperature', 'humidity']
            writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
            if not exists:
                writer.writeheader()
            writer.writerow({'date_time': local_date_str, 'temperature': temperature_c, 'humidity': humidity})
            csvfile.close()

    except (RuntimeError, Exception) as error:
        time.sleep(2.0)
        continue

    time.sleep(1200.0)
