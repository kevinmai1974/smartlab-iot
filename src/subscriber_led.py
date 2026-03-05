import json
import os
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
TOPIC_CMD = f"{BASE}/actuators/led/cmd"
TOPIC_STATE = f"{BASE}/actuators/led/state"
TOPIC_ONLINE = f"{BASE}/status/online"

CLIENT_ID = f"{TEAM}-{DEVICE}-led"

LED_GPIO = int(os.getenv("LED_GPIO", "17"))

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

# GPIO: vrai sur Raspberry Pi, faux sinon (pour tester sur PC)
GPIO_AVAILABLE = True
try:
    import RPi.GPIO as GPIO
except Exception:
    GPIO_AVAILABLE = False

def gpio_setup():
    if not GPIO_AVAILABLE:
        print("[GPIO] RPi.GPIO not available -> simulation mode")
        return
    GPIO.setmode(GPIO.BCM)
    GPIO.setwarnings(False)
    GPIO.setup(LED_GPIO, GPIO.OUT)
    GPIO.output(LED_GPIO, GPIO.LOW)

def gpio_write(state: str):
    if not GPIO_AVAILABLE:
        print(f"[GPIO] simulate LED -> {state}")
        return
    GPIO.output(LED_GPIO, GPIO.HIGH if state == "on" else GPIO.LOW)

def publish_state(c: mqtt.Client, state: str):
    payload = {
        "device": DEVICE,
        "actuator": "led",
        "state": state,
        "ts": iso_utc_now(),
    }
    c.publish(TOPIC_STATE, json.dumps(payload), qos=1, retain=True)
    print(f"[STATE] {TOPIC_STATE} -> {payload}")

client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)

if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

client.will_set(TOPIC_ONLINE, payload="offline", qos=1, retain=True)

def on_connect(c, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] connected")
        c.publish(TOPIC_ONLINE, "online", qos=1, retain=True)
        c.subscribe(TOPIC_CMD, qos=1)
        print(f"[SUB] {TOPIC_CMD}")
    else:
        print(f"[MQTT] connect error rc={rc}")

def on_message(c, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="replace")
    print(f"[MSG] topic={msg.topic} qos={msg.qos} retain={msg.retain} payload={raw}")

    # Robustesse: JSON invalide => log + ignore
    try:
        data = json.loads(raw)
    except Exception:
        print("[WARN] invalid JSON -> ignored")
        return

    # On accepte soit {"state":"on"} soit {"cmd":"on"} etc.
    state = (data.get("state") or data.get("cmd") or "").lower().strip()
    if state not in ("on", "off"):
        print("[WARN] invalid state -> expected on/off")
        return

    gpio_write(state)
    publish_state(c, state)

client.on_connect = on_connect
client.on_message = on_message

gpio_setup()

try:
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever()
except KeyboardInterrupt:
    pass
finally:
    try:
        client.publish(TOPIC_ONLINE, "offline", qos=1, retain=True)
        client.disconnect()
    except Exception:
        pass
    if GPIO_AVAILABLE:
        GPIO.cleanup()