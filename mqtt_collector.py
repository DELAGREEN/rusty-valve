#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import db_queue

MQTT_BROKER = "localhost"
MQTT_TOPICS = [("plc/raw/data", 1), ("plc/command/qr_code", 1)]

db_queue.init_db()
db_queue.start_cleanup_scheduler()   # запускаем автоматическую очистку

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Collector: подключен к MQTT")
        for topic, qos in MQTT_TOPICS:
            client.subscribe(topic, qos)
            print(f"   Подписан на {topic} (QoS {qos})")
    else:
        print(f"❌ Collector: ошибка {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8', errors='replace')
    db_queue.enqueue_message(msg.topic, payload)
    print(f"📥 Сохранено: {msg.topic} -> {payload[:60]}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)

print("Collector запущен, жду сообщения...")
client.loop_forever()