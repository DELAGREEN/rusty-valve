#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import json
import struct

MQTT_BROKER = "localhost"
MQTT_TOPIC_RAW = "plc/raw/data"
MQTT_TOPIC_COMMAND = "plc/command/raw"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Подключен к брокеру, подписываемся на топик", MQTT_TOPIC_RAW)
        client.subscribe(MQTT_TOPIC_RAW)
    else:
        print("Ошибка подключения")

def on_message(client, userdata, msg):
    if msg.topic == MQTT_TOPIC_RAW:
        # Получили сырые данные от ПЛК
        raw_data = msg.payload.decode()
        try:
            # Ожидаем формат "temp_raw,volume_raw"
            parts = raw_data.split(',')
            temp_raw = int(parts[0])
            volume_raw = int(parts[1])
            temp_c = temp_raw / 10.0
            print(f"\n[Данные от ПЛК] Температура: {temp_c:.1f} °C, Объём: {volume_raw} л")
        except:
            print(f"[ВНИМАНИЕ] Неизвестный формат сырых данных: {raw_data}")

def send_command(temp_raw, volume_raw):
    """Отправляет сырые данные в топик команд для ПЛК"""
    payload = f"{temp_raw},{volume_raw}"
    client.publish(MQTT_TOPIC_COMMAND, payload)
    print(f"Команда отправлена: {payload}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, 1883, 60)
client.loop_start()

print("Обработчик запущен. Для отправки команды введите: temp_raw,volume_raw")
print("Пример: 500,100  (температура 50.0°C, объём 100л)")

try:
    while True:
        cmd = input("> ")
        if cmd.strip():
            parts = cmd.split(',')
            if len(parts) == 2:
                send_command(int(parts[0]), int(parts[1]))
            else:
                print("Неверный формат. Нужно: целое,целое")
except KeyboardInterrupt:
    print("\nВыход")
    client.loop_stop()
    client.disconnect()