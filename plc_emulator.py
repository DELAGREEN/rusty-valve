#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import random
import time
import json
import threading

# --- НАСТРОЙКИ ---
MQTT_BROKER = "localhost"
MQTT_TOPIC_RAW_DATA = "plc/raw/data"
MQTT_TOPIC_QR_COMMAND = "plc/command/qr_code" # Тот же топик, что и в обработчике
SLAVE_ID = 1
# -----------------

# --- Функция для публикации телеметрии ---
def publish_data():
    client = mqtt.Client()
    client.connect(MQTT_BROKER, 1883, 60)
    print("🏭 Эмулятор ПЛК запущен. Публикую сырые данные...")
    while True:
        temp_raw = random.randint(0, 1000)   # температура * 10 (0.0 ... 100.0)
        volume_raw = random.randint(0, 1000)  # объём в литрах
        payload = f"{temp_raw},{volume_raw}"

        client.publish(MQTT_TOPIC_RAW_DATA, payload)
        print(f"[MQTT->PLC] Опубликовано: temp_raw={temp_raw} ({temp_raw/10:.1f}°C), volume_raw={volume_raw} л")
        time.sleep(5)

# --- MQTT Callback для получения команд ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("🏭 ПЛК подключился к MQTT")
        # Подписываемся на топик с QR-командами
        client.subscribe(MQTT_TOPIC_QR_COMMAND)
    else:
        print(f"Ошибка подключения MQTT, код: {rc}")

def on_message(client, userdata, msg):
    # Обрабатываем входящие команды
    if msg.topic == MQTT_TOPIC_QR_COMMAND:
        qr_content = msg.payload.decode()
        print(f"\n🏭 *************************")
        print(f"🏭 [СОБЫТИЕ] ПЛК ПОЛУЧИЛ QR-КОМАНДУ!")
        print(f"🏭 Содержимое QR-кода: {qr_content}")
        print(f"🏭 *************************\n")

# --- Запускаем подписчика MQTT в отдельном потоке ---
def start_mqtt_listener():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever()

# --- Запуск всех потоков ---
if __name__ == "__main__":
    # Запускаем слушателя команд в фоновом потоке
    mqtt_thread = threading.Thread(target=start_mqtt_listener, daemon=True)
    mqtt_thread.start()

    # Запускаем основной цикл публикации данных
    publish_data()