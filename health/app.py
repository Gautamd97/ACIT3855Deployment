import json
import logging
import logging.config
from datetime import datetime
from pathlib import Path

import connexion
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion import NoContent

with open("/app/config/log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f.read())
    
logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

with open("/app/config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f.read())

STATUS_FILE = Path(APP_CONF["datastore"]["filename"])
CHECK_INTERVAL = APP_CONF["scheduler"]["period_sec"]

SERVICES = APP_CONF["services"]  


def _now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_status():
    if STATUS_FILE.exists():
        try:
            with STATUS_FILE.open("r") as f:
                return json.load(f)
        except Exception:
            logger.exception("Failed to read status file, resetting")
    return {}


def _save_status(data):
    tmp = STATUS_FILE.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(data, f)
    tmp.replace(STATUS_FILE)


def check_all_services():
    logger.info("Health service: running periodic checks")
    data = _load_status()
    data.setdefault("services", {})
    data["last_checked"] = _now_iso()

    for name, info in SERVICES.items():
        url = info["url"]
        try:
            r = requests.get(url, timeout=3)
            status = "up" if r.status_code == 200 else "degraded"
            detail = r.json() if "application/json" in r.headers.get("Content-Type", "") else {}
            logger.info("Health check %s -> %s", name, status)
        except Exception as e:
            status = "down"
            detail = {"error": str(e)}
            logger.warning("Health check failed for %s: %s", name, e)

        data["services"][name] = {
            "status": status,
            "checked_at": _now_iso(),
            "detail": detail,
        }

    _save_status(data)

def get_overall_status():
    """Return last known status snapshot."""
    data = _load_status()
    if not data:
        return {"message": "No status collected yet"}, 404
    return data, 200


def get_health():
    """Health of THIS service."""
    return {
        "status": "healthy",
        "timestamp": _now_iso()
    }, 200


app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yml", strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(check_all_services, "interval", seconds=CHECK_INTERVAL)
    sched.start()
    logger.info("Health check scheduler started")
    app.run(port=8120, host="0.0.0.0")