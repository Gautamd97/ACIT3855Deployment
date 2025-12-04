import json
import uuid
from datetime import datetime
import logging
import logging.config

import connexion
import yaml
from connexion import NoContent
from pykafka import KafkaClient

with open("/app/config/log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f.read())
    
logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

with open("/app/config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f.read())

STORAGE_URL = APP_CONF.get("storage", {}).get("url")

KAFKA_HOSTS = f"{APP_CONF['events']['hostname']}:{APP_CONF['events']['port']}"
KAFKA_TOPIC = APP_CONF['events']['topic'].encode()

_KAFKA_CLIENT = None
_PRODUCER_ADM = None
_PRODUCER_CAP = None


def _trace_id() -> str:
    return str(uuid.uuid4())

def _require_items(body: dict, kind: str):
    items = body.get("items")
    if not items or not isinstance(items, list):
        logger.error("Receiver: %s batch has no items[] — nothing to forward", kind)
        return None
    return items

def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def _get_producer(cache_key: str):
    global _KAFKA_CLIENT, _PRODUCER_ADM, _PRODUCER_CAP

    if _KAFKA_CLIENT is None:
        logger.info("Receiver: creating global Kafka client to %s", KAFKA_HOSTS)
        _KAFKA_CLIENT = KafkaClient(hosts=KAFKA_HOSTS)

    if cache_key == "adm":
        if _PRODUCER_ADM is None:
            topic = _KAFKA_CLIENT.topics[KAFKA_TOPIC]
            _PRODUCER_ADM = topic.get_sync_producer()
            logger.info("Receiver: created admission producer for topic=%s", KAFKA_TOPIC.decode())
        return _PRODUCER_ADM
    else:
        if _PRODUCER_CAP is None:
            topic = _KAFKA_CLIENT.topics[KAFKA_TOPIC]
            _PRODUCER_CAP = topic.get_sync_producer()
            logger.info("Receiver: created capacity producer for topic=%s", KAFKA_TOPIC.decode())
        return _PRODUCER_CAP


def report_admission_discharge_batch(body):
    trace = _trace_id()
    items = _require_items(body, "admission/discharge")
    if items is None:
        return NoContent, 400

    logger.info("Receiver: admission/discharge batch trace_id=%s items=%d", trace, len(items))

    meta = {
        "batchId": body.get("batchId"),
        "senderId": body.get("senderId"),
        "reportDate": body.get("reportDate"),
        "sentAt": body.get("sentAt"),
        "version": body.get("version"),
        "trace_id": trace,
    }

    required = ("encounterId", "event", "recordedAt", "patientAge")

    try:
        producer = _get_producer("adm")
    except Exception as e:
        logger.exception("Receiver couldn't connect to Kafka (%s)", e)
        return NoContent, 503

    for i, item in enumerate(items, start=1):
        missing = [k for k in required if k not in item]
        if missing:
            logger.error("Item #%d missing required fields: %s", i, ", ".join(missing))
            return NoContent, 400

        try:
            patient_age = int(item["patientAge"])
        except Exception:
            logger.error("Item #%d has non-integer patientAge: %r", i, item.get("patientAge"))
            return NoContent, 400

        payload = {
            **meta,
            "encounterId": item["encounterId"],
            "event": item["event"],
            "recordedAt": item["recordedAt"],
            "patientAge": patient_age,
        }

        event = {
            "type": "admission_created",
            "datetime": _now_iso(),
            "payload": payload
        }

        try:
            producer.produce(json.dumps(event).encode("utf-8"))
            logger.info("→ Kafka topic=%s trace_id=%s payload=%s",
                        KAFKA_TOPIC.decode(), payload.get("trace_id"), payload)
        except Exception as e:
            logger.exception("Receiver couldn't publish to Kafka (%s)", e)
            return NoContent, 503

    return NoContent, 201


def report_capacity_batch(body):
    trace = _trace_id()
    items = _require_items(body, "capacity")
    if items is None:
        return NoContent, 400

    logger.info("Receiver: capacity batch trace_id=%s items=%d", trace, len(items))

    meta = {
        "batchId": body.get("batchId"),
        "senderId": body.get("senderId"),
        "reportDate": body.get("reportDate"),
        "sentAt": body.get("sentAt"),
        "version": body.get("version"),
        "trace_id": trace,
    }

    required = ("unitId", "totalBeds", "occupiedBeds", "recordedAt")

    try:
        producer = _get_producer("cap")   # 👉 uses global client + cached producer
    except Exception as e:
        logger.exception("Receiver couldn't connect to Kafka (%s)", e)
        return NoContent, 503

    for i, item in enumerate(items, start=1):
        missing = [k for k in required if k not in item]
        if missing:
            logger.error("Capacity item #%d missing required fields: %s", i, ", ".join(missing))
            return NoContent, 400

        try:
            total_beds = int(item["totalBeds"])
            occupied_beds = int(item["occupiedBeds"])
        except Exception:
            logger.error("Capacity item #%d has non-integer totals: totalBeds=%r occupiedBeds=%r",
                         i, item.get("totalBeds"), item.get("occupiedBeds"))
            return NoContent, 400

        payload = {
            **meta,
            "unitId": item["unitId"],
            "totalBeds": total_beds,
            "occupiedBeds": occupied_beds,
            "recordedAt": item["recordedAt"],
        }

        event = {
            "type": "capacity_snapshot",
            "datetime": _now_iso(),
            "payload": payload
        }

        try:
            producer.produce(json.dumps(event).encode("utf-8"))
            logger.info("→ Kafka topic=%s trace_id=%s payload=%s",
                        KAFKA_TOPIC.decode(), payload.get("trace_id"), payload)
        except Exception as e:
            logger.exception("Receiver couldn't publish to Kafka (%s)", e)
            return NoContent, 503

    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yml", strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
