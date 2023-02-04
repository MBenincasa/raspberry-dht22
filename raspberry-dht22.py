import time
import board
import adafruit_dht

dhtDevice = adafruit_dht.DHT22(board.D22)

while True:
    try:
        temperature = dhtDevice.temperature
        humidity = dhtDevice.humidity
        print(
            "Temperature: {:.1f} C    Humidity: {}% ".format(
                temperature, humidity
            )
        )

    except (RuntimeError, Exception) as error:
        print(error.args[0])
        time.sleep(2.0)
        continue

    time.sleep(5.0)
