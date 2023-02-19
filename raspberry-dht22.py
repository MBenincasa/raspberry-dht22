import time
import board
import adafruit_dht

dhtDevice = adafruit_dht.DHT22(board.D22)

while True:
    try:
        temperature_c = dhtDevice.temperature
        humidity = dhtDevice.humidity
        local_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("{} -  T: {:.1f} C    H: {}% ".format(local_date, temperature_c, humidity))

    except (RuntimeError, Exception) as error:
        print(error.args[0])
        time.sleep(1.0)
        continue

    time.sleep(5.0)
    