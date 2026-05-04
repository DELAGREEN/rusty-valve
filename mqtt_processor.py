#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import cv2
import numpy as np          # для работы с координатами полигонов
from pyzbar import pyzbar
import threading
import time

# ---------- НАСТРОЙКИ MQTT ----------
MQTT_BROKER = "localhost"
MQTT_TOPIC_RAW_DATA = "plc/raw/data"
MQTT_TOPIC_QR_COMMAND = "plc/command/qr_code"
# -----------------------------------

# Глобальная переменная, чтобы не отправлять один и тот же код повторно
last_qr_code = ""

# ---------- MQTT Callbacks ----------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✓ MQTT подключен, подписываемся на {MQTT_TOPIC_RAW_DATA}")
        client.subscribe(MQTT_TOPIC_RAW_DATA)
    else:
        print(f"✗ Ошибка MQTT подключения, код: {rc}")

def on_message(client, userdata, msg):
    # Обработка входящих данных от PLC (температура, объём)
    if msg.topic == MQTT_TOPIC_RAW_DATA:
        raw_data = msg.payload.decode()
        try:
            parts = raw_data.split(',')
            temp_raw = int(parts[0])
            volume_raw = int(parts[1])
            temp_c = temp_raw / 10.0
            print(f"\n[Данные от ПЛК] Температура: {temp_c:.1f} °C, Объём: {volume_raw} л")
        except Exception as e:
            print(f"[!] Неизвестный формат данных: {raw_data} ({e})")

# Создаём MQTT клиента и настраиваем
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)
client.loop_start()   # запускаем фоновый поток для приёма сообщений

# ---------- Функция для работы с камерой (запускается в отдельном потоке) ----------
def camera_loop():
    global last_qr_code
    # Пытаемся открыть веб-камеру (0 — встроенная, можно попробовать 1 если внешняя)
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("❌ Ошибка: Не удалось открыть веб-камеру. Проверь права доступа.")
        return

    print("📸 Окно камеры открыто. Наведи объектив на QR-код (любой текст).")
    print("   Нажми 'q' в окне камеры для выхода.")

    while True:
        ret, frame = cap.read()
        if not ret:
            print("❌ Не удалось получить кадр с камеры.")
            break

        # Ищем QR-коды на текущем кадре
        decoded_objects = pyzbar.decode(frame)

        for obj in decoded_objects:
            # Получаем текст из QR-кода (может быть любой текст, не только URL)
            qr_text = obj.data.decode('utf-8')
            # Получаем координаты углов QR-кода (полигон)
            points = obj.polygon

            # Рисуем зелёную рамку вокруг QR-кода
            if len(points) == 4:
                # Преобразуем точки в формат, понятный OpenCV
                pts = [(point.x, point.y) for point in points]
                pts = np.array(pts, np.int32)
                cv2.polylines(frame, [pts], True, (0, 255, 0), 3)

                # Подпись над рамкой (первые 30 символов, чтобы не забивать экран)
                label = f"QR: {qr_text[:30]}{'...' if len(qr_text) > 30 else ''}"
                # Координата для текста — левый верхний угол рамки
                text_x = pts[0][0]
                text_y = pts[0][1] - 10
                cv2.putText(frame, label, (text_x, text_y),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

            # Логика отправки в MQTT только для новых кодов
            if qr_text != last_qr_code:
                last_qr_code = qr_text
                print(f"\n🎉 Обнаружен новый QR-код: {qr_text}")
                print(f"📤 Отправляю команду в MQTT (топик: {MQTT_TOPIC_QR_COMMAND})")
                # Публикуем распознанный текст в MQTT
                client.publish(MQTT_TOPIC_QR_COMMAND, qr_text)

        # Показываем видео с нарисованными рамками
        cv2.imshow('QR Scanner', frame)

        # Выход по клавише 'q'
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    # Освобождаем ресурсы
    cap.release()
    cv2.destroyAllWindows()
    print("📸 Работа с камерой завершена.")

# ---------- Запуск потока с камерой ----------
camera_thread = threading.Thread(target=camera_loop, daemon=True)
camera_thread.start()

print("Обработчик запущен. Нажми Ctrl+C в терминале для полного выхода.")
# Держим основной поток активным
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nОстанавливаем программу...")
    client.loop_stop()
    client.disconnect()