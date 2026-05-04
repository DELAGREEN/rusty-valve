#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import random
import time
import json
import struct

MQTT_BROKER = "localhost"
MQTT_TOPIC_RAW = "plc/raw/data"
SLAVE_ID = 1

client = mqtt.Client()
client.connect(MQTT_BROKER, 1883, 60)

print("Эмулятор ПЛК запущен, публикуем сырые данные...")

while True:
    # Имитируем сырые значения с Modbus устройства
    temp_raw = random.randint(0, 1000)   # температура * 10 (0.0 ... 100.0)
    volume_raw = random.randint(0, 1000)  # объём в литрах

    # Вариант 1: отправить как два числа в строке через запятую (просто для отладки)
    payload_str = f"{temp_raw},{volume_raw}"
    
    # Вариант 2: отправить как бинарные данные (2 регистра = 4 байта, big-endian)
    # payload_bytes = struct.pack('>HH', temp_raw, volume_raw)
    
    client.publish(MQTT_TOPIC_RAW, payload_str)
    print(f"Опубликовано: temp_raw={temp_raw} ({temp_raw/10:.1f}°C), volume_raw={volume_raw} л")
    time.sleep(5)