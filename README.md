# Raspberry Pi - DHT22

## Introduction
This repository will contain code written in the Python programming language to make the DHT22 sensor communicate with a Raspberry Pi. There are two different scripts that read data from the sensor.<p>
The script [raspberry-dht22.py](https://github.com/MBenincasa/raspberry-dht22/blob/main/raspberry-dht22.py) reads data from the sensor and writes to CSV files there. One file is created for each day.<p>
The script [raspberry-dht22-google-sheet.py](https://github.com/MBenincasa/raspberry-dht22/blob/main/raspberry-dht22-google-sheet.py) reads the data from the sensor and then writes them on a previously created Google Sheet.
Then I wrote a Google App Script to expose an API that returned data in JSON format.<p>
Bash scripts are present to be able to run the two scripts in the background on a Linux operating system

## Hardware
For this project I'm using a Raspberry Pi 3 model B and an AZDelivery DHT22 sensor. The sensor is connected to the raspberry via jumpers.

## Prerequisites
### Script that reads data and saves it to CSV
You'll also need to install a library to communicate with the DHT sensor. In this case, we're going to install the CircuitPython_DHT library. This library works with both the DHT22 and DHT11 sensors.
```batch
pip3 install adafruit-circuitpython-dht
```
```batch
sudo apt-get install libgpiod2
```
### Script that reads data and adds it to a Google Sheet
In addition to installing the Adafruit library that allows us to read data from the sensor, it will be necessary to install dependencies in order to use Google services.<p>
[https://developers.google.com/sheets/api/quickstart/python](https://developers.google.com/sheets/api/quickstart/python)
### Script that reads data and adds it to an AWS dynamoDB table
In addition to installing the Adafruit library which allows us to read data from the sensor, it will be necessary to install dependencies to be able to use AWS dynamoDB services.<p>
[https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)