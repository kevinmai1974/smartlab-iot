import json
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import pymysql
from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

TEAM = os.getenv("TEAM", "team01")
DEVICE = os.getenv("DEVICE", "pi01")

BASE = f"ahuntsic/aec-iot/b3/{TEAM}/{DEVICE}"
TOPIC_SENSOR = f"{BASE}/sensors/temperature"
TOPIC_STATE = f"{BASE}/actuators/led/state"
TOPIC_ONLINE = f"{BASE}/status/online"

CLIENT_ID = f"{TEAM}-{DEVICE}-logger"

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "smartlab")
DB_PASS = os.getenv("DB_PASS", "smartlabpass")
DB_NAME = os.getenv("DB_NAME", "smartlab")

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def db_connect():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

def insert_telemetry(conn, device: str, topic: str, value, unit: str, ts_utc: str):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO telemetry(device, topic, value, unit, ts_utc) VALUES (%s,%s,%s,%s,%s)",
            (device, topic, value, unit, ts_utc),
        )

def insert_event(conn, device: str, topic: str, payload_text: str, ts_utc: str):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO events(device, topic, payload, ts_utc) VALUES (%s,%s,%s,%s)",
            (device, topic, payload_text, ts_utc),
        )

conn = None

def ensure_conn():
    global conn
    if conn is None:
        conn = db_connect()
    return conn

client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

def on_connect(c, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] connected")
        c.subscribe([(TOPIC_SENSOR, 0), (TOPIC_STATE, 1), (TOPIC_ONLINE, 1)])
        print("[SUB] telemetry/state/online")
    else:
        print(f"[MQTT] connect error rc={rc}")

def on_message(c, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="replace")
    topic = msg.topic
    print(f"[LOG] {topic} payload={raw}")

    # Online topic: log comme event simple
    if topic == TOPIC_ONLINE:
        try:
            ensure_conn()
            insert_event(conn, DEVICE, topic, raw, iso_utc_now())
        except Exception as e:
            print(f"[DB] error online insert: {e}")
            conn_close()
        return

    # Sensor/state: JSON attendu
    try:
        data = json.loads(raw)
    except Exception:
        print("[WARN] invalid JSON -> ignored (no crash)")
        return

    try:
        ensure_conn()

        if topic == TOPIC_SENSOR:
            value = data.get("value", None)
            unit = data.get("unit", None)
            ts = data.get("ts", iso_utc_now())
            insert_telemetry(conn, data.get("device", DEVICE), topic, value, unit, ts)

        else:
            # state topic -> events
            ts = data.get("ts", iso_utc_now())
            insert_event(conn, data.get("device", DEVICE), topic, raw, ts)

    except Exception as e:
        print(f"[DB] insert error: {e}")
        conn_close()

def conn_close():
    global conn
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass
    conn = None

client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever()
except KeyboardInterrupt:
    pass
finally:
    try:
        client.disconnect()
    except Exception:
        pass
    conn_close()