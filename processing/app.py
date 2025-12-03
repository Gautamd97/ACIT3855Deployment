import json
import logging
import logging.config
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import connexion
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from pathlib import Path

from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


with open("/app/config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f)

with open("/app/config/log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f)


logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

STATS_FILE = APP_CONF["datastore"]["filename"]
INTERVAL   = int(APP_CONF["scheduler"]["interval"])

ADMISSIONS_URL = APP_CONF["eventstores"]["admissions"]["url"]
CAPACITY_URL   = APP_CONF["eventstores"]["capacity"]["url"]

def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _load_stats() -> Dict[str, Any]:
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "num_admission_events": 0,
            "num_capacity_snapshots": 0,
            "max_patient_age": None,
            "max_occupied_beds": None,
            "last_updated": "1970-01-01T00:00:00Z",  
        }

def _save_stats(stats: Dict[str, Any]) -> None:
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)


def _latest_ts(
        items: List[Dict[str, Any]], 
        ts_key: str = "recorded_at"
        ) -> Optional[str]:
    latest = None
    for it in items:
        ts = it.get(ts_key)
        if not ts:
            continue
        try:
            _ = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        latest = ts if latest is None else (ts if ts > latest else latest)
    return latest


def populate_stats():
    """Periodic job: fetch ALL events from storage and recompute stats."""
    logger.info("Periodic processing started")

    # 1. Fixed wide time window that covers all lab data
    params = {
        "start_timestamp": "1970-01-01T00:00:00Z",
        "end_timestamp":   "2100-01-01T00:00:00Z",
    }
    logger.info("Fetching ALL events from storage with params=%s", params)

    try:
        ra = requests.get(ADMISSIONS_URL, params=params, timeout=5)
        rc = requests.get(CAPACITY_URL,   params=params, timeout=5)
    except Exception as e:
        logger.error("Failed to call storage endpoints: %s", e, exc_info=True)
        logger.info("Periodic processing ended (errors)")
        return

    if ra.status_code != 200:
        logger.error("Admissions GET returned %s", ra.status_code)
        admissions: List[Dict[str, Any]] = []
    else:
        admissions = ra.json()

    if rc.status_code != 200:
        logger.error("Capacity GET returned %s", rc.status_code)
        capacity: List[Dict[str, Any]] = []
    else:
        capacity = rc.json()

    logger.info(
        "Recomputing stats from %d admissions, %d capacity records",
        len(admissions), len(capacity),
    )

    # 2. Recompute counters from scratch
    num_admission_events = len(admissions)
    num_capacity_snapshots = len(capacity)

    # 3. Compute maxima. These keys MUST match what storage returns.
    # If your storage returns patientAge/occupiedBeds instead,
    # change the keys below accordingly.
    new_ages = [
        int(x["patient_age"])
        for x in admissions
        if "patient_age" in x and x["patient_age"] is not None
    ]
    max_patient_age = max(new_ages) if new_ages else None

    new_occ = [
        int(x["occupied_beds"])
        for x in capacity
        if "occupied_beds" in x and x["occupied_beds"] is not None
    ]
    max_occupied_beds = max(new_occ) if new_occ else None

    stats = {
        "num_admission_events": num_admission_events,
        "num_capacity_snapshots": num_capacity_snapshots,
        "max_patient_age": max_patient_age,
        "max_occupied_beds": max_occupied_beds,
        "last_updated": _iso_now(),
    }

    _save_stats(stats)
    logger.debug("Updated stats: %s", stats)
    logger.info("Periodic processing ended")

def get_stats():
    logger.info("GET /stats requested")

    if not Path(STATS_FILE).exists():
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404

    stats = _load_stats()
    logger.debug("Stats payload: %s", stats)
    logger.info("GET /stats completed")
    return stats, 200


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, "interval", seconds=INTERVAL)
    sched.start()
    logger.info("Scheduler started (interval=%ss)", INTERVAL)

app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yml", strict_validation=True, validate_responses=False)


app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")