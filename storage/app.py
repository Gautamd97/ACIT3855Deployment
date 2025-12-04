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
from pykafka.common import OffsetType          # PART 3: for consumer options
from pykafka.exceptions import KafkaException  # PART 3: to catch Kafka-specific errors
from models import AdmissionDischarge, Capacity, Base
from database import ENGINE
import time
from sqlalchemy.exc import OperationalError
from models import Base
from database import ENGINE


with open("/app/config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f.read())

db_conf = APP_CONF["datastore"]
DB_ENGINE_STRING = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

KAFKA_HOST = APP_CONF["events"]["hostname"]
KAFKA_PORT = APP_CONF["events"]["port"]
KAFKA_TOPIC = APP_CONF["events"]["topic"].encode()

_KAFKA_CLIENT = None
_CONSUMER = None

with open("/app/config/log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

ENGINE = create_engine(
    DB_ENGINE_STRING,
    echo=False,
    future=True,
    pool_size=5,         
    pool_recycle=3600,   
    pool_pre_ping=True  
)
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


def _get_consumer():

    global _KAFKA_CLIENT, _CONSUMER

    if _CONSUMER is not None:
        return _CONSUMER

    try:
        logger.info("Storage: creating Kafka client to %s:%s", KAFKA_HOST, KAFKA_PORT)
        _KAFKA_CLIENT = KafkaClient(hosts=f"{KAFKA_HOST}:{KAFKA_PORT}")

        topic = _KAFKA_CLIENT.topics[KAFKA_TOPIC]
        _CONSUMER = topic.get_simple_consumer(
            reset_offset_on_start=False,
            auto_offset_reset=OffsetType.LATEST,
        )
        logger.info("Storage: Kafka consumer created for topic=%s", KAFKA_TOPIC.decode())
        return _CONSUMER

    except KafkaException as e:
        logger.warning("Storage: error creating Kafka consumer: %s", e)
        _KAFKA_CLIENT = None
        _CONSUMER = None
        return None


def process_messages():
    """
    Background loop that reads from Kafka forever.
    If Kafka goes down, we catch the error, reset the consumer,
    wait a bit, and retry without killing the service.
    """
    global _KAFKA_CLIENT, _CONSUMER

    logger.info("Storage: starting Kafka consumer loop")

    while True:
        consumer = _get_consumer()
        if consumer is None:
            logger.info("Storage: Kafka unavailable, retrying in 5 seconds...")
            time.sleep(5)
            continue

        try:
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
                    logger.exception("Storage: error processing Kafka message: %s", e)

        except KafkaException as e:
            logger.warning("Storage: exception in Kafka consumer loop: %s", e)
            try:
                if _CONSUMER is not None:
                    _CONSUMER.stop()
            except Exception:
                pass

            _CONSUMER = None
            _KAFKA_CLIENT = None

            logger.info("Storage: will retry Kafka connection in 5 seconds...")
            time.sleep(5)


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


def init_db(max_retries: int = 10, delay: int = 5):
    """Ensure all tables exist in the target database, retrying until DB is ready."""
    attempt = 1
    while attempt <= max_retries:
        try:
            logger.info("Initializing DB (attempt %s/%s)...", attempt, max_retries)
            Base.metadata.create_all(ENGINE)
            logger.info("Database tables ensured/created successfully.")
            return
        except OperationalError as e:
            logger.warning(
                "DB not ready yet (attempt %s/%s): %s. Retrying in %ss...",
                attempt, max_retries, e, delay
            )
            time.sleep(delay)
            attempt += 1
        except Exception as e:
            logger.exception("Unexpected error during init_db: %s", e)
            break

    logger.error("Could not initialize DB after %s attempts. Continuing without DB.", max_retries)


app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yml", strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    init_db()

    t = Thread(target=process_messages)
    t.daemon = True
    t.start()

    logger.info("Background Kafka consumer thread started")
    app.run(port=8090, host="0.0.0.0")