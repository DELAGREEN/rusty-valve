#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import cv2
import numpy as np
from pyzbar import pyzbar
import threading
import time
import db_queue   # наш модуль очереди

# ---------- НАСТРОЙКИ MQTT ----------
MQTT_BROKER = "localhost"
MQTT_TOPIC_QR_COMMAND = "plc/command/qr_code"   # только для отправки команд
# -----------------------------------

# Инициализируем БД и запускаем автоочистку
db_queue.init_db()
db_queue.start_cleanup_scheduler()

# Глобальные переменные
last_qr_code = ""
current_temp = 0.0
current_volume = 0

# MQTT клиент только для отправки (не для приёма)
mqtt_pub = mqtt.Client()
mqtt_pub.connect(MQTT_BROKER, 1883, 60)
mqtt_pub.loop_start()

# ---------- ПОТОК ЧТЕНИЯ ИЗ ОЧЕРЕДИ ----------
def queue_worker():
    global current_temp, current_volume
    while True:
        msg = db_queue.dequeue_message()
        if msg:
            # Обрабатываем сообщение из БД
            if msg['topic'] == "plc/raw/data":
                try:
                    parts = msg['payload'].split(',')
                    temp_raw = int(parts[0])
                    volume_raw = int(parts[1])
                    current_temp = temp_raw / 10.0
                    current_volume = volume_raw
                    print(f"\n[Данные от ПЛК] Температура: {current_temp:.1f} °C, Объём: {current_volume} л")
                except Exception as e:
                    print(f"[!] Ошибка данных: {e}")
            # Можно обработать и другие топики, если нужно
            db_queue.mark_done(msg['id'])
        else:
            time.sleep(0.3)

queue_thread = threading.Thread(target=queue_worker, daemon=True)
queue_thread.start()

# ---------- ФУНКЦИЯ КАМЕРЫ (без изменений, только отправка через mqtt_pub) ----------
def camera_loop():
    global last_qr_code
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("❌ Ошибка: Не удалось открыть веб-камеру.")
        return

    print("📸 Окно камеры открыто. Нажми 'q' для выхода.")

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        decoded_objects = pyzbar.decode(frame)

        for obj in decoded_objects:
            qr_text = obj.data.decode('utf-8')
            points = obj.polygon

            if len(points) == 4:
                pts = [(point.x, point.y) for point in points]
                pts = np.array(pts, np.int32)
                cv2.polylines(frame, [pts], True, (0, 255, 0), 3)

                label = f"QR: {qr_text[:30]}{'...' if len(qr_text) > 30 else ''}"
                text_x = pts[0][0]
                text_y = pts[0][1] - 10
                cv2.putText(frame, label, (text_x, text_y),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

            if qr_text != last_qr_code:
                last_qr_code = qr_text
                print(f"\n🎉 Обнаружен новый QR-код: {qr_text}")
                print(f"📤 Отправляю команду в MQTT (топик: {MQTT_TOPIC_QR_COMMAND})")
                mqtt_pub.publish(MQTT_TOPIC_QR_COMMAND, qr_text)

        # Показываем текущие данные поверх видео
        info = f"Temp: {current_temp:.1f}C  Vol: {current_volume}L"
        cv2.putText(frame, info, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2)

        cv2.imshow('QR Scanner + PLC Data', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()
    print("📸 Работа с камерой завершена.")

# ---------- ЗАПУСК ----------
camera_thread = threading.Thread(target=camera_loop, daemon=True)
camera_thread.start()

print("Обработчик запущен. Нажми Ctrl+C для выхода.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nОстанавливаем программу...")
    mqtt_pub.loop_stop()
    mqtt_pub.disconnect()