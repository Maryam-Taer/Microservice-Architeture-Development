import json
import yaml
import logging
import datetime
import requests
import connexion
from logging import config
from pykafka import KafkaClient
from connexion import NoContent
from flask_cors import CORS, cross_origin

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def get_searched_restaurants(index):
    """ Get BP Reading in History """
    hostname = f'{app_config["events"]["hostname"]}:{app_config["events"]["port"]}'
        
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    # Here we reset the offset on start so that we retrieve
    # messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to
    # 100ms. There is a risk that this loop never stops if the
    # index is large and messages are constantly being received!

    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
    logger.info(f"Retrieving restaurant at index {index}")

    count = 0
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)

            # Find the event at the index you want and
            # return code 200
            # i.e., return event, 200

            if msg["type"] == "fr":
                if count == index:
                    body = msg["payload"]
                    return body, 200

                count += 1
    except:
        logger.error("No more messages found")

    logger.error(f"Could not find restaurant at index {index}")
    return {"message": "Not Found"}, 404


def get_posted_reviews(index):
    """ Get BP Reading in History """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
    logger.info(f"Retrieving review at index {index}")

    count = 0
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)

            if msg["type"] == "wr":
                if count == index:
                    body = msg["payload"]
                    return body, 200

                count += 1
    except:
        logger.error("No more messages found")

    logger.error(f"Could not find review at index {index}")
    return {"message": "Not Found"}, 404


app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content_Type'
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # run our standalone gevent server
    app.run(port=8110, host="0.0.0.0", use_reloader=False)
