import yaml
import logging
from logging import config
import connexion
from connexion import NoContent
import requests
import datetime
import json
from pykafka import KafkaClient


YAML_FILE = "openapi.yaml"
EVENT_FILE = "events.json"
MAX_EVENTS: int = 12
OUTPUT = []


STORAGE_URL = "http://localhost:8090"

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def find_restaurant(body) -> NoContent:
    """ Receives a request to find a restaurant """
    # headers = {"Content_Type": "application/json"}
    # response = requests.post(app_config["FindRestaurant"]["url"], json=body, headers=headers)

    logger.info(f'Received event "Find Restaurant" request with a unique id of {body["Restaurant_id"]}')

    client = KafkaClient(hosts='acit3855-setc.eastus.cloudapp.azure.com:9092')
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    producer = topic.get_sync_producer()

    msg = {"type": "fr",
           "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
           "payload": body}

    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(f'Returned event "Find Restaurant" response (id: {body["Restaurant_id"]}) with status code 201.')

    return NoContent, 201


def write_review(body) -> NoContent:
    """ Receives a review event """
    # headers = {"Content_Type": "application/json"}
    # response = requests.post(app_config["WriteReview"]["url"], json=body, headers=headers)

    logger.info(f'Received event "Write Review" request with a unique id of {body["Post_id"]}')

    client = KafkaClient(hosts='acit3855-setc.eastus.cloudapp.azure.com:9092')
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    producer = topic.get_sync_producer()
    msg = {"type": "wr", "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(f'Returned event "Write Review" response (id: {body["Post_id"]}) with status code 201.')

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(YAML_FILE, strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)
