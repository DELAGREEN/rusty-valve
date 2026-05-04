# db_queue.py
import sqlite3
import threading
import time
from datetime import datetime, timedelta

DB_PATH = "mqtt_queue.db"
CLEANUP_INTERVAL_SECONDS = 86400  # раз в сутки
OLD_MESSAGE_DAYS = 7              # хранить сообщения 7 дней

def init_db():
    """Создаёт таблицу и индексы, если их нет."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            payload TEXT,
            received_at TEXT,
            status TEXT DEFAULT 'new'
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_status ON messages(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_received ON messages(received_at)')
    conn.commit()
    conn.close()

def enqueue_message(topic, payload):
    """Сохраняет новое сообщение в очередь."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'INSERT INTO messages (topic, payload, received_at, status) VALUES (?, ?, ?, ?)',
        (topic, payload, datetime.now().isoformat(), 'new')
    )
    conn.commit()
    conn.close()

def dequeue_message():
    """Извлекает одно самое старое 'new' сообщение и помечает как 'processing'."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('BEGIN IMMEDIATE')
    c.execute(
        'SELECT id, topic, payload FROM messages WHERE status = "new" ORDER BY id LIMIT 1'
    )
    row = c.fetchone()
    if row:
        msg_id, topic, payload = row
        c.execute('UPDATE messages SET status = "processing" WHERE id = ?', (msg_id,))
        conn.commit()
        conn.close()
        return {'id': msg_id, 'topic': topic, 'payload': payload}
    conn.commit()
    conn.close()
    return None

def mark_done(msg_id):
    """Помечает сообщение как обработанное."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('UPDATE messages SET status = "done" WHERE id = ?', (msg_id,))
    conn.commit()
    conn.close()

def cleanup_old_messages():
    """Удаляет сообщения со статусом 'done' старше OLD_MESSAGE_DAYS дней."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=OLD_MESSAGE_DAYS)).isoformat()
    c.execute('DELETE FROM messages WHERE status = "done" AND received_at < ?', (cutoff,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    if deleted:
        print(f"[Очистка] Удалено {deleted} старых сообщений.")
    return deleted

def start_cleanup_scheduler():
    """Запускает фоновый поток, который раз в сутки вызывает cleanup_old_messages."""
    def scheduler_loop():
        while True:
            time.sleep(CLEANUP_INTERVAL_SECONDS)
            cleanup_old_messages()
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()