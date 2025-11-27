import json
import logging
import logging.config

import connexion
import yaml
from pykafka import KafkaClient
from connexion import NoContent

with open("log_conf.yml", "r") as f:
    LOG_CONF = yaml.safe_load(f)
logging.config.dictConfig(LOG_CONF)
logger = logging.getLogger("basicLogger")

with open("app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f)

KAFKA_HOSTS = f"{APP_CONF['events']['hostname']}:{APP_CONF['events']['port']}"
KAFKA_TOPIC = APP_CONF["events"]["topic"].encode()


def _get_consumer():
    """
    Returns a simple consumer that starts reading from the beginning
    of the Kafka topic and times out once it has read all messages.
    """
    client = KafkaClient(hosts=KAFKA_HOSTS)
    topic = client.topics[KAFKA_TOPIC]
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000
    )
    return consumer


def _parse_index(index_str):
    try:
        idx = int(index_str)
        if idx < 0:
            raise ValueError
        return idx
    except Exception:
        return None

def get_admission_event(index):
    """
    Returns the admission/discharge event (type 'admission_created')
    at per-type index `index`, or 404 if not found.
    """
    idx = _parse_index(index)
    if idx is None:
        return {"message": "index must be a non-negative integer"}, 400

    logger.info("GET /hospital/admission-from-queue?index=%s", idx)

    consumer = _get_consumer()

    admission_counter = 0
    for msg in consumer:
        if msg is None:
            continue

        try:
            data = json.loads(msg.value.decode("utf-8"))
        except Exception:
            logger.warning("Skipping non-JSON message: %r", msg.value)
            continue

        etype = data.get("type")
        payload = data.get("payload", {})

        if etype == "admission_created":
            if admission_counter == idx:
                logger.info("Found admission event at index %d", idx)
                return payload, 200
            admission_counter += 1

    logger.info("No admission event at index %d", idx)
    return {"message": f"No admission event at index {idx}!"}, 404

def get_capacity_event(index):
    """
    Returns the capacity event (type 'capacity_snapshot')
    at per-type index `index`, or 404 if not found.
    """
    idx = _parse_index(index)
    if idx is None:
        return {"message": "index must be a non-negative integer"}, 400

    logger.info("GET /hospital/capacity-from-queue?index=%s", idx)

    consumer = _get_consumer()

    capacity_counter = 0
    for msg in consumer:
        if msg is None:
            continue

        try:
            data = json.loads(msg.value.decode("utf-8"))
        except Exception:
            logger.warning("Skipping non-JSON message: %r", msg.value)
            continue

        etype = data.get("type")
        payload = data.get("payload", {})

        if etype == "capacity_snapshot":
            if capacity_counter == idx:
                logger.info("Found capacity event at index %d", idx)
                return payload, 200
            capacity_counter += 1

    logger.info("No capacity event at index %d", idx)
    return {"message": f"No capacity event at index {idx}!"}, 404

def get_stats():
    """
    Scans the whole queue and returns counts of each event type.
    """
    logger.info("GET /stats requested")

    consumer = _get_consumer()

    num_admission_events = 0
    num_capacity_events = 0

    for msg in consumer:
        if msg is None:
            continue

        try:
            data = json.loads(msg.value.decode("utf-8"))
        except Exception:
            logger.warning("Skipping non-JSON message: %r", msg.value)
            continue

        etype = data.get("type")
        if etype == "admission_created":
            num_admission_events += 1
        elif etype == "capacity_snapshot":
            num_capacity_events += 1

    stats = {
        "num_admission_events": num_admission_events,
        "num_capacity_events": num_capacity_events,
    }

    logger.info(
        "Stats computed: %s admission, %s capacity",
        num_admission_events, num_capacity_events
    )
    return stats, 200

app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yml", strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")