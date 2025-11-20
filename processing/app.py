import json
import logging
import logging.config
from datetime import datetime, timezone
from typing import Dict, Any, List
import connexion
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from pathlib import Path

with open("app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f)

with open("log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f)
logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

STATS_FILE = APP_CONF["datastore"]["filename"]
INTERVAL   = APP_CONF["scheduler"]["interval"]

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

def _latest_ts(items: List[Dict[str, Any]], ts_key: str = "recorded_at") -> str | None:
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
    logger.info("Periodic processing started")

    stats = _load_stats()
    start = stats["last_updated"]
    end = _iso_now()

    params = {"start_timestamp": start, "end_timestamp": end}
    logger.info("Fetching new events in window [%s, %s)", start, end)

    try:
        ra = requests.get(ADMISSIONS_URL, params=params, timeout=5)
        rc = requests.get(CAPACITY_URL,   params=params, timeout=5)
    except Exception as e:
        logger.error("Failed to call storage endpoints: %s", e)
        logger.info("Periodic processing ended (errors)")
        return

    if ra.status_code != 200:
        logger.error("Admissions GET returned %s", ra.status_code)
        admissions = []
    else:
        admissions = ra.json()

    if rc.status_code != 200:
        logger.error("Capacity GET returned %s", rc.status_code)
        capacity = []
    else:
        capacity = rc.json()

    logger.info("New admissions: %d, new capacity: %d", len(admissions), len(capacity))

    stats["num_admission_events"]   = int(stats["num_admission_events"]) + len(admissions)
    stats["num_capacity_snapshots"] = int(stats["num_capacity_snapshots"]) + len(capacity)

    new_ages = [int(x["patient_age"]) for x in admissions if "patient_age" in x and x["patient_age"] is not None]
    if new_ages:
        stats["max_patient_age"] = max(
            [a for a in [stats.get("max_patient_age")] if a is not None] + new_ages
        )

    new_occ = [int(x["occupied_beds"]) for x in capacity if "occupied_beds" in x and x["occupied_beds"] is not None]
    if new_occ:
        stats["max_occupied_beds"] = max(
            [o for o in [stats.get("max_occupied_beds")] if o is not None] + new_occ
        )

    stats["last_updated"] = end

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

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")