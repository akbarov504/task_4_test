import requests, time, math
from threading import Lock, Thread
from datetime import datetime, timezone
from flask import Flask, jsonify, request

app = Flask(__name__)

SOURCE_CAN_API_URL = "http://127.0.0.1:8080/api/telemetry"
GPS_API_URL = "http://127.0.0.1:5000/api/gps/info"

POLL_INTERVAL_SECONDS = 3
SPEED_THRESHOLD_MPH = 5.0

DEFAULT_SETTINGS = {
    "movement_validation_duration": 10,
    "stop_duration_threshold": 20,
    "idle_time_limit": 60,
}

def utc_now():
    return datetime.now(timezone.utc)

def parse_ts(value):
    if value is None:
        return utc_now()

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str):
        value = value.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)

    raise ValueError("Invalid timestamp")

def ts_to_iso(dt):
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def haversine_miles(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2]:
        return 0.0

    r = 3958.7613
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def fetch_json(url, timeout=10):
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()

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

    lat = gps_data.get("lat", gps_data.get("latitude"))
    lon = gps_data.get("lon", gps_data.get("longitude"))

    return {
        "speed_mph": safe_float(speed_mph, 0.0),
        "lat": lat,
        "lon": lon,
    }

def fetch_source_can_data():
    telemetry_data = fetch_json(SOURCE_CAN_API_URL, timeout=10)
    gps_data = fetch_json(GPS_API_URL, timeout=10)

    if isinstance(telemetry_data, list):
        telemetry_data = telemetry_data[0] if telemetry_data else None

    if not isinstance(telemetry_data, dict):
        return None

    gps_fields = extract_gps_fields(gps_data)

    combined = dict(telemetry_data)
    combined["speed_mph"] = gps_fields["speed_mph"]
    combined["lat"] = gps_fields["lat"]
    combined["lon"] = gps_fields["lon"]

    return combined

class SingleTruckProcessor:
    def __init__(self):
        self.lock = Lock()
        self.events = []
        self.last_poll_status = {
            "success": None,
            "last_poll_time": None,
            "last_error": None,
            "item_received": False,
        }

        self.state = {
            "asset_state": "STATIONARY",
            "settings": DEFAULT_SETTINGS.copy(),

            "last_timestamp": None,
            "last_speed_mph": 0.0,
            "last_latitude": None,
            "last_longitude": None,
            "last_odometer": None,
            "last_engine_status": "OFF",

            "last_processed_timestamp": None,

            "move_candidate_start_time": None,
            "move_candidate_latitude": None,
            "move_candidate_longitude": None,
            "move_candidate_odometer": None,

            "stop_candidate_start_time": None,
            "stop_candidate_latitude": None,
            "stop_candidate_longitude": None,
            "stop_candidate_odometer": None,

            "trip_start_time": None,
            "trip_start_latitude": None,
            "trip_start_longitude": None,
            "trip_start_odometer": None,

            "idle_candidate_start_time": None,
            "idle_candidate_latitude": None,
            "idle_candidate_longitude": None,
            "idle_active": False,

            "total_idle_seconds": 0
        }

    def add_event(self, event):
        self.events.append(event)
        if len(self.events) > 10000:
            self.events = self.events[-10000:]

    def calculate_trip_distance(self, stop_odometer, stop_latitude, stop_longitude):
        start_odometer = self.state.get("trip_start_odometer")

        if start_odometer is not None and stop_odometer is not None:
            diff = safe_float(stop_odometer) - safe_float(start_odometer)
            return round(max(diff, 0.0), 3)

        return round(
            haversine_miles(
                self.state.get("trip_start_latitude"),
                self.state.get("trip_start_longitude"),
                stop_latitude,
                stop_longitude,
            ),
            3
        )

    def process_item(self, item):
        with self.lock:
            timestamp = parse_ts(item.get("timestamp"))
            speed_mph = safe_float(item.get("speed_mph", 0))
            latitude = item.get("lat")
            longitude = item.get("lon")
            current_odometer = item.get("current_odometer")
            engine_status = str(item.get("engine_status", "OFF")).upper()

            current_ts_iso = ts_to_iso(timestamp)

            if self.state["last_processed_timestamp"] == current_ts_iso:
                return []

            self.state["last_processed_timestamp"] = current_ts_iso

            created_events = []

            if speed_mph > SPEED_THRESHOLD_MPH:
                if self.state["move_candidate_start_time"] is None:
                    self.state["move_candidate_start_time"] = timestamp
                    self.state["move_candidate_latitude"] = latitude
                    self.state["move_candidate_longitude"] = longitude
                    self.state["move_candidate_odometer"] = current_odometer

                duration_above = (timestamp - self.state["move_candidate_start_time"]).total_seconds()

                if (
                    self.state["asset_state"] == "STATIONARY"
                    and duration_above >= self.state["settings"]["movement_validation_duration"]
                ):
                    event = {
                        "event_type": "START_MOVING",
                        "timestamp": ts_to_iso(self.state["move_candidate_start_time"]),
                        "location": {
                            "latitude": self.state["move_candidate_latitude"],
                            "longitude": self.state["move_candidate_longitude"],
                        },
                        "current_odometer": self.state["move_candidate_odometer"],
                    }
                    self.add_event(event)
                    created_events.append(event)

                    self.state["asset_state"] = "MOVING"
                    self.state["trip_start_time"] = self.state["move_candidate_start_time"]
                    self.state["trip_start_latitude"] = self.state["move_candidate_latitude"]
                    self.state["trip_start_longitude"] = self.state["move_candidate_longitude"]
                    self.state["trip_start_odometer"] = self.state["move_candidate_odometer"]

                    self.state["stop_candidate_start_time"] = None
                    self.state["stop_candidate_latitude"] = None
                    self.state["stop_candidate_longitude"] = None
                    self.state["stop_candidate_odometer"] = None

                    self.state["idle_candidate_start_time"] = None
                    self.state["idle_candidate_latitude"] = None
                    self.state["idle_candidate_longitude"] = None
                    self.state["idle_active"] = False
            else:
                self.state["move_candidate_start_time"] = None
                self.state["move_candidate_latitude"] = None
                self.state["move_candidate_longitude"] = None
                self.state["move_candidate_odometer"] = None

            if self.state["asset_state"] == "MOVING":
                if speed_mph < SPEED_THRESHOLD_MPH:
                    if self.state["stop_candidate_start_time"] is None:
                        self.state["stop_candidate_start_time"] = timestamp
                        self.state["stop_candidate_latitude"] = latitude
                        self.state["stop_candidate_longitude"] = longitude
                        self.state["stop_candidate_odometer"] = current_odometer

                    below_duration = (timestamp - self.state["stop_candidate_start_time"]).total_seconds()

                    if below_duration >= self.state["settings"]["stop_duration_threshold"]:
                        stop_time = self.state["stop_candidate_start_time"]

                        if self.state["trip_start_time"]:
                            trip_duration = int((stop_time - self.state["trip_start_time"]).total_seconds())
                            trip_duration = max(trip_duration, 0)
                        else:
                            trip_duration = 0

                        trip_distance = self.calculate_trip_distance(
                            stop_odometer=current_odometer,
                            stop_latitude=latitude,
                            stop_longitude=longitude,
                        )

                        event = {
                            "event_type": "STOP_MOVING",
                            "timestamp": ts_to_iso(stop_time),
                            "duration": trip_duration,
                            "final_location": {
                                "latitude": latitude,
                                "longitude": longitude,
                            },
                            "trip_distance": trip_distance,
                        }
                        self.add_event(event)
                        created_events.append(event)

                        self.state["asset_state"] = "STATIONARY"

                        self.state["trip_start_time"] = None
                        self.state["trip_start_latitude"] = None
                        self.state["trip_start_longitude"] = None
                        self.state["trip_start_odometer"] = None

                        self.state["stop_candidate_start_time"] = None
                        self.state["stop_candidate_latitude"] = None
                        self.state["stop_candidate_longitude"] = None
                        self.state["stop_candidate_odometer"] = None
                else:
                    self.state["stop_candidate_start_time"] = None
                    self.state["stop_candidate_latitude"] = None
                    self.state["stop_candidate_longitude"] = None
                    self.state["stop_candidate_odometer"] = None

            idle_condition = engine_status == "ON" and speed_mph < SPEED_THRESHOLD_MPH

            if idle_condition:
                if self.state["idle_candidate_start_time"] is None:
                    self.state["idle_candidate_start_time"] = timestamp
                    self.state["idle_candidate_latitude"] = latitude
                    self.state["idle_candidate_longitude"] = longitude
                    self.state["idle_active"] = False
                else:
                    idle_duration = (timestamp - self.state["idle_candidate_start_time"]).total_seconds()
                    if idle_duration >= self.state["settings"]["idle_time_limit"]:
                        self.state["idle_active"] = True
            else:
                if self.state["idle_candidate_start_time"] is not None and self.state["idle_active"]:
                    idle_total = int((timestamp - self.state["idle_candidate_start_time"]).total_seconds())
                    idle_total = max(idle_total, 0)

                    event = {
                        "event_type": "ENGINE_IDLE",
                        "idle_start_timestamp": ts_to_iso(self.state["idle_candidate_start_time"]),
                        "duration": idle_total,
                        "location": {
                            "latitude": self.state["last_latitude"],
                            "longitude": self.state["last_longitude"],
                        },
                    }
                    self.add_event(event)
                    created_events.append(event)

                    self.state["total_idle_seconds"] += idle_total

                self.state["idle_candidate_start_time"] = None
                self.state["idle_candidate_latitude"] = None
                self.state["idle_candidate_longitude"] = None
                self.state["idle_active"] = False

            self.state["last_timestamp"] = timestamp
            self.state["last_speed_mph"] = speed_mph
            self.state["last_latitude"] = latitude
            self.state["last_longitude"] = longitude
            self.state["last_odometer"] = current_odometer
            self.state["last_engine_status"] = engine_status

            return created_events

    def get_state(self):
        with self.lock:
            return {
                "asset_state": self.state["asset_state"],
                "settings": self.state["settings"],
                "total_idle_seconds": self.state["total_idle_seconds"],
                "trip_start_time": ts_to_iso(self.state["trip_start_time"]),
                "idle_candidate_start_time": ts_to_iso(self.state["idle_candidate_start_time"]),
                "last_sample": {
                    "timestamp": ts_to_iso(self.state["last_timestamp"]),
                    "speed_mph": self.state["last_speed_mph"],
                    "latitude": self.state["last_latitude"],
                    "longitude": self.state["last_longitude"],
                    "current_odometer": self.state["last_odometer"],
                    "engine_status": self.state["last_engine_status"],
                }
            }

    def get_events(self, limit=100):
        with self.lock:
            return self.events[-limit:]

    def reset(self):
        with self.lock:
            current_settings = self.state["settings"].copy()
            self.events = []

            self.state = {
                "asset_state": "STATIONARY",
                "settings": current_settings,

                "last_timestamp": None,
                "last_speed_mph": 0.0,
                "last_latitude": None,
                "last_longitude": None,
                "last_odometer": None,
                "last_engine_status": "OFF",

                "last_processed_timestamp": None,

                "move_candidate_start_time": None,
                "move_candidate_latitude": None,
                "move_candidate_longitude": None,
                "move_candidate_odometer": None,

                "stop_candidate_start_time": None,
                "stop_candidate_latitude": None,
                "stop_candidate_longitude": None,
                "stop_candidate_odometer": None,

                "trip_start_time": None,
                "trip_start_latitude": None,
                "trip_start_longitude": None,
                "trip_start_odometer": None,

                "idle_candidate_start_time": None,
                "idle_candidate_latitude": None,
                "idle_candidate_longitude": None,
                "idle_active": False,

                "total_idle_seconds": 0
            }

processor = SingleTruckProcessor()

def poll_source_api_forever():
    while True:
        try:
            item = fetch_source_can_data()
            print(item)
            if item:
                created_events = processor.process_item(item)
                processor.last_poll_status = {
                    "success": True,
                    "last_poll_time": ts_to_iso(utc_now()),
                    "last_error": None,
                    "item_received": True,
                    "events_created_count": len(created_events),
                }
            else:
                processor.last_poll_status = {
                    "success": True,
                    "last_poll_time": ts_to_iso(utc_now()),
                    "last_error": None,
                    "item_received": False,
                    "events_created_count": 0,
                }

        except Exception as e:
            processor.last_poll_status = {
                "success": False,
                "last_poll_time": ts_to_iso(utc_now()),
                "last_error": str(e),
                "item_received": False,
                "events_created_count": 0,
            }

        time.sleep(POLL_INTERVAL_SECONDS)

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "success": True,
        "service": "single-truck-movement-idle-detector",
        "poll_interval_seconds": POLL_INTERVAL_SECONDS
    }), 200

@app.route("/api/poll-status", methods=["GET"])
def poll_status():
    return jsonify({
        "success": True,
        "poll_status": processor.last_poll_status
    }), 200

@app.route("/api/state", methods=["GET"])
def state():
    return jsonify({
        "success": True,
        "state": processor.get_state()
    }), 200

@app.route("/api/events", methods=["GET"])
def events():
    limit = int(request.args.get("limit", 100))
    return jsonify({
        "success": True,
        "events": processor.get_events(limit=limit)
    }), 200

@app.route("/api/process-now", methods=["POST"])
def process_now():
    try:
        item = fetch_source_can_data()

        if not item:
            return jsonify({
                "success": True,
                "message": "No data received from source API",
                "events_created_count": 0,
                "events_created": []
            }), 200

        created_events = processor.process_item(item)

        return jsonify({
            "success": True,
            "events_created_count": len(created_events),
            "events_created": created_events
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

@app.route("/api/reset", methods=["POST"])
def reset():
    processor.reset()
    return jsonify({
        "success": True,
        "message": "Processor state reset successfully"
    }), 200

if __name__ == "__main__":
    poller_thread = Thread(target=poll_source_api_forever, daemon=True)
    poller_thread.start()

    app.run(port=2222)
