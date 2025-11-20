import json
import logging
import logging.config
from threading import Thread
from dateutil import parser
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
import connexion
from connexion import NoContent
import yaml
from pykafka import KafkaClient
from models import AdmissionDischarge, Capacity

with open("app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f.read())

db_conf = APP_CONF["datastore"]
DB_ENGINE_STRING = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

KAFKA_HOST = APP_CONF["events"]["hostname"]
KAFKA_PORT = APP_CONF["events"]["port"]
KAFKA_TOPIC = APP_CONF["events"]["topic"].encode()

with open("log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

ENGINE = create_engine(DB_ENGINE_STRING, echo=False, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)

def _parse_dt(s: str):
    return parser.isoparse(s)

def _parse_date(s: str):
    return parser.isoparse(s).date()

def _date_col(model):
    return getattr(model, "date_created", getattr(model, "recorded_at"))

def _row_to_dict(obj):
    return {k: v for k, v in obj.__dict__.items() if k != "_sa_instance_state"}

def create_admission_discharge(body):
    with SessionLocal() as session:
        try:
            row = AdmissionDischarge(
                batch_id=body["batchId"],
                sender_id=body["senderId"],
                report_date=_parse_date(body["reportDate"]),
                sent_at=_parse_dt(body["sentAt"]),
                version=body["version"],
                encounter_id=body["encounterId"],
                event=body["event"],
                recorded_at=_parse_dt(body["recordedAt"]),
                patient_age=int(body["patientAge"]),
                trace_id=body["trace_id"],
            )
            session.add(row)
            session.commit()
            logger.info("Stored admission/discharge trace_id=%s", body["trace_id"])
            return NoContent, 201
        except Exception as e:
            session.rollback()
            logger.exception("Failed to store admission/discharge: %s", e)
            return NoContent, 400

def create_capacity(body):
    with SessionLocal() as session:
        try:
            row = Capacity(
                batch_id=body["batchId"],
                sender_id=body["senderId"],
                report_date=_parse_date(body["reportDate"]),
                sent_at=_parse_dt(body["sentAt"]),
                version=body["version"],
                unit_id=body["unitId"],
                total_beds=int(body["totalBeds"]),
                occupied_beds=int(body["occupiedBeds"]),
                recorded_at=_parse_dt(body["recordedAt"]),
                trace_id=body["trace_id"],
            )
            session.add(row)
            session.commit()
            logger.info("Stored capacity trace_id=%s", body["trace_id"])
            return NoContent, 201
        except Exception as e:
            session.rollback()
            logger.exception("Failed to store capacity: %s", e)
            return NoContent, 400

def process_messages():
    logger.info("Starting Kafka consumer (process_messages)")
    try:
        client = KafkaClient(hosts=f"{KAFKA_HOST}:{KAFKA_PORT}")
        topic = client.topics[KAFKA_TOPIC]
        consumer = topic.get_simple_consumer()
    except Exception as e:
        logger.exception("Failed to connect to Kafka: %s", e)
        return

    for msg in consumer:
        if msg is None:
            continue
        try:
            message = json.loads(msg.value.decode("utf-8"))
            etype = message.get("type")
            payload = message.get("payload", {})

            logger.info("Kafka message received type=%s", etype)

            if etype == "admission_created":
                create_admission_discharge(payload)
            elif etype == "capacity_snapshot":
                create_capacity(payload)
            else:
                logger.warning("Unknown message type: %s", etype)
        except Exception as e:
            logger.exception("Error processing Kafka message: %s", e)


def get_admission_readings(start_timestamp, end_timestamp):
    try:
        start = _parse_dt(start_timestamp)
        end = _parse_dt(end_timestamp)
    except Exception:
        return {"message": "Invalid timestamp format"}, 400

    with SessionLocal() as session:
        col = _date_col(AdmissionDischarge)
        rows = session.execute(
            select(AdmissionDischarge).where(col >= start, col < end)
        ).scalars().all()
        return [_row_to_dict(r) for r in rows], 200

def get_capacity_readings(start_timestamp, end_timestamp):
    try:
        start = _parse_dt(start_timestamp)
        end = _parse_dt(end_timestamp)
    except Exception:
        return {"message": "Invalid timestamp format"}, 400

    with SessionLocal() as session:
        col = _date_col(Capacity)
        rows = session.execute(
            select(Capacity).where(col >= start, col < end)
        ).scalars().all()
        return [_row_to_dict(r) for r in rows], 200


app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yml", strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    t = Thread(target=process_messages)
    t.daemon = True
    t.start()
    logger.info("Background Kafka consumer thread started")
    app.run(port=8090,host="0.0.0.0")