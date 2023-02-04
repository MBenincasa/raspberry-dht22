# Raspberry Pi - DHT22

## Introduction
This repository will contain code written in the Python programming language to make the DHT22 sensor communicate with a Raspberry Pi

## Hardware
For this project I'm using a Raspberry Pi 3 model B and an AZDelivery DHT22 sensor. The sensor is connected to the raspberry via jumpers.

## Prerequisites
You'll also need to install a library to communicate with the DHT sensor. In this case, we're going to install the CircuitPython_DHT library. This library works with both the DHT22 and DHT11 sensors.
```batch
pip3 install adafruit-circuitpython-dht
```
```batch
sudo apt-get install libgpiod2
```
