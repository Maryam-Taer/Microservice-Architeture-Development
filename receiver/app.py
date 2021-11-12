import yaml
import json
import time
import logging
from logging import config
import connexion
from connexion import NoContent
import requests
import datetime
from pykafka import KafkaClient


YAML_FILE = "openapi.yaml"

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Trying to connect to Kafka
hostname = f'{app_config["events"]["hostname"]}:{app_config["events"]["port"]}'
    
max_connection_retry = app_config["events"]["max_retries"]
current_retry_count = 0

while current_retry_count < max_connection_retry:
    try:
        logger.info(f'[Retry #{current_retry_count}] Connecting to Kafka...')
        
        client = KafkaClient(hosts=hostname)
        topic = client.topics[str.encode(app_config["events"]["topic"])]
        break
        
    except:
        logger.error(f'Connection to Kafka failed in retry #{current_retry_count}!')
        time.sleep(app_config["events"]["sleep"])
        current_retry_count += 1


def find_restaurant(body) -> NoContent:
    """ Receives a request to find a restaurant """
    # headers = {"Content_Type": "application/json"}
    # response = requests.post(app_config["FindRestaurant"]["url"], json=body, headers=headers)

    logger.info(f'Received event "Find Restaurant" request with a unique id of {body["Restaurant_id"]}')

#     hostname = f'{app_config["events"]["hostname"]}:{app_config["events"]["port"]}'
#     client = KafkaClient(hosts=hostname)
#     topic = client.topics[str.encode(app_config["events"]["topic"])]

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
    producer = topic.get_sync_producer()
    
    msg = {"type": "wr", 
           "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), 
           "payload": body}
    
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(f'Returned event "Write Review" response (id: {body["Post_id"]}) with status code 201.')

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(YAML_FILE, strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)
