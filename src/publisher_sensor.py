import json
import os
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")

TEAM = os.getenv("TEAM", "team01")
DEVICE = os.getenv("DEVICE", "pi01")

BASE = f"ahuntsic/aec-iot/b3/{TEAM}/{DEVICE}"
TOPIC_JSON = f"{BASE}/sensors/temperature"
TOPIC_VALUE = f"{BASE}/sensors/temperature/value"
TOPIC_ONLINE = f"{BASE}/status/online"

CLIENT_ID = f"{TEAM}-{DEVICE}-publisher"

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def read_cpu_temp_c() -> float | None:
    path = "/sys/class/thermal/thermal_zone0/temp"
    try:
        with open(path, "r", encoding="utf-8") as f:
            milli = int(f.read().strip())
        return milli / 1000.0
    except Exception:
        return None

def make_payload(temp_c: float) -> dict:
    return {
        "device": DEVICE,
        "sensor": "temperature",
        "value": round(temp_c, 2),
        "unit": "C",
        "ts": iso_utc_now(),
    }

client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)

if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

# LWT: offline retained
client.will_set(TOPIC_ONLINE, payload="offline", qos=1, retain=True)

def on_connect(c, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] connected")
        # Online retained
        c.publish(TOPIC_ONLINE, "online", qos=1, retain=True)
    else:
        print(f"[MQTT] connect error rc={rc}")

def on_disconnect(c, userdata, rc):
    print(f"[MQTT] disconnected rc={rc}")

client.on_connect = on_connect
client.on_disconnect = on_disconnect

client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
client.loop_start()

try:
    while True:
        temp = read_cpu_temp_c()
        if temp is None:
            # fallback simple si pas sur Pi
            temp = 22.0

        payload = make_payload(temp)
        client.publish(TOPIC_JSON, json.dumps(payload), qos=0, retain=False)
        client.publish(TOPIC_VALUE, str(payload["value"]), qos=0, retain=False)
        print(f"[PUB] {TOPIC_JSON} -> {payload}")

        time.sleep(5)  # 2 � 10 sec (ici 5)
except KeyboardInterrupt:
    pass
finally:
    # laisser LWT g�rer offline si coupure brutale, mais on peut publier offline proprement
    client.publish(TOPIC_ONLINE, "offline", qos=1, retain=True)
    client.loop_stop()
    client.disconnect()