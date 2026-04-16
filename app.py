import time
import requests, uuid
from threading import Thread, Lock
from datetime import datetime, timezone
from flask import Flask, jsonify
from token_manager import get_valid_token

app = Flask(__name__)

SOURCE_CAN_API_URL = "http://127.0.0.1:8080/api/telemetry"
GPS_API_URL = "http://127.0.0.1:5000/api/gps/info"

POLL_INTERVAL_SECONDS = 2
SPEED_THRESHOLD_MPH = 5.0
IDLE_TIME_LIMIT = 10

def utc_now():
    return datetime.now(timezone.utc)

def ts_to_iso(dt):
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_ts(value):
    if value is None:
        return utc_now()

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str):
        value = value.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return utc_now()

    return utc_now()

def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def fetch_json(url, timeout=10):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}

def extract_gps_fields(gps_data):
    if not isinstance(gps_data, dict):
        return {
            "speed_mph": 0.0,
            "lat": None,
            "lon": None,
        }

    speed_mph = (
        gps_data.get("speed_mph")
        or gps_data.get("speed")
        or gps_data.get("speedInMph")
        or gps_data.get("speed_in_mph")
        or 0.0
    )

    lat = gps_data.get("lat", gps_data.get("latitude", 0))
    lon = gps_data.get("lon", gps_data.get("longitude", 0))

    return {
        "speed_mph": safe_float(speed_mph, 0.0),
        "lat": lat,
        "lon": lon,
    }

def fetch_combined_data():
    telemetry_data = fetch_json(SOURCE_CAN_API_URL, timeout=10)
    gps_data = fetch_json(GPS_API_URL, timeout=10)

    if isinstance(telemetry_data, list):
        telemetry_data = telemetry_data[0] if telemetry_data else None

    if not isinstance(telemetry_data, dict):
        return None

    gps_fields = extract_gps_fields(gps_data)
    item = dict(telemetry_data)
    item["speed_mph"] = gps_fields["speed_mph"]
    item["lat"] = gps_fields["lat"]
    item["lon"] = gps_fields["lon"]

    return item

class SimpleEventDetector:
    def __init__(self):
        self.lock = Lock()

        self.asset_state = "STOPPED"
        self.last_event = {
            "event_type": "STOPPED",
            "timestamp": None,
            "speed_mph": 0.0,
            "engine_status": "OFF",
            "lat": None,
            "lon": None,
            "current_odometer": None,
            "message": "Initial state"
        }

        self.last_data = None
        self.idle_start_time = None

    def process(self, item):
        with self.lock:
            timestamp = parse_ts(item.get("timestamp"))
            speed_mph = safe_float(item.get("speed_mph"), 0.0)
            lat = item.get("lat")
            lon = item.get("lon")
            current_odometer = item.get("total_distance")
            engine_status = str(item.get("status", "OFF")).upper()

            prev_state = self.asset_state
            event_type = None
            message = ""

            is_moving = speed_mph > SPEED_THRESHOLD_MPH
            is_engine_on = engine_status == "ON"
            if is_moving:
                if prev_state != "MOVING":
                    event_type = "START_MOVING"
                    self.asset_state = "MOVING"
                    message = "Vehicle started moving"
                else:
                    event_type = "MOVING"
                    self.asset_state = "MOVING"
                    message = "Vehicle is moving"

                self.idle_start_time = None
            else:
                if is_engine_on:
                    if self.idle_start_time is None:
                        self.idle_start_time = timestamp

                    idle_seconds = (timestamp - self.idle_start_time).total_seconds()
                    if idle_seconds >= IDLE_TIME_LIMIT:
                        event_type = "ENGINE_IDLE"
                        self.asset_state = "ENGINE_IDLE"
                        message = f"Engine idle for {int(idle_seconds)} seconds"
                    else:
                        event_type = "STOPPED"
                        self.asset_state = "STOPPED"
                        message = f"Vehicle stopped, idle timer running ({int(idle_seconds)} sec)"
                else:
                    self.idle_start_time = None
                    event_type = "STOPPED"
                    self.asset_state = "STOPPED"
                    message = "Vehicle stopped and engine off"
            event = {
                "event_type": event_type,
                "timestamp": ts_to_iso(timestamp),
                "speed_mph": round(speed_mph, 2),
                "engine_status": engine_status,
                "lat": lat,
                "lon": lon,
                "current_odometer": current_odometer,
                "state": self.asset_state,
                "message": message
            }

            if prev_state == "MOVING" and self.asset_state != "MOVING" and event_type == "STOPPED":
                event["event_type"] = "STOPPED"
                event["message"] = "Vehicle stopped moving"

            self.last_event = event
            self.last_data = item

            print("\n================ CURRENT EVENT ================")
            print("timestamp      :", event["timestamp"])
            print("event_type     :", event["event_type"])
            print("state          :", event["state"])
            print("speed_mph      :", event["speed_mph"])
            print("engine_status  :", event["engine_status"])
            print("lat            :", event["lat"])
            print("lon            :", event["lon"])
            print("current_odometer:", event["current_odometer"])
            print("message        :", event["message"])
            print("==============================================\n")

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {get_valid_token()}"
            }

            body = {
                "globalEventId": f"GL-EVENT-{uuid.uuid4()}",
                "event": event["event_type"],
                "deviceDateTime": event["timestamp"],
                "latitude": event["lat"],
                "longitude": event["lon"],
                "state": "AR",
                "location": "Arzon State",
                "direction": "NW",
                "fuelLevelPercent": 12,
                "defLevelPercent": 12,
                "speed": event["speed_mph"]
            }

            requests.post("https://dev-gw.tracksafe365.com/services/glssafety/api/truck-events/send", headers=headers, json=body, timeout=5)

            return event

    def get_current_event(self):
        with self.lock:
            return self.last_event

detector = SimpleEventDetector()

def poll_forever():
    while True:
        try:
            item = fetch_combined_data()
            print("RAW DATA:", item)

            if item:
                detector.process(item)
            else:
                print("RAW DATA: None")

        except Exception as e:
            print("ERROR:", str(e))

        time.sleep(POLL_INTERVAL_SECONDS)

@app.route("/api/current-event", methods=["GET"])
def current_event():
    return jsonify({
        "success": True,
        "current_event": detector.get_current_event()
    }), 200

if __name__ == "__main__":
    print("Service starting...")
    print("Telemetry API:", SOURCE_CAN_API_URL)
    print("GPS API:", GPS_API_URL)
    print("Poll interval:", POLL_INTERVAL_SECONDS)
    print("Speed threshold:", SPEED_THRESHOLD_MPH)
    print("Idle limit:", IDLE_TIME_LIMIT)

    poll_thread = Thread(target=poll_forever, daemon=True)
    poll_thread.start()

    app.run(port=2222)