import json
import yaml
import logging
import datetime
import connexion
from logging import config
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from write_review import WriteReview
from find_restaurant import FindingRestaurant
from threading import Thread
from pykafka import KafkaClient
from pykafka.common import OffsetType

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')
logger.info(f'Connecting to DB. Hostname: {app_config["datastore"]["hostname"]}, '
            f'Port: {app_config["datastore"]["port"]}')

DB_ENGINE = create_engine(f'mysql+pymysql://{app_config["datastore"]["user"]}:'
                          f'{app_config["datastore"]["password"]}@{app_config["datastore"]["hostname"]}:'
                          f'{app_config["datastore"]["port"]}/{app_config["datastore"]["db"]}')

Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)


def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])

    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).

    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)

    # This is blocking - it will wait for a new message
    for msg in consumer:
        print(msg)
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)

        payload = msg["payload"]

        if msg["type"] == "fr":  # Change this to your event type
            # Store the event1 (i.e., the payload) to the DB
            # find_restaurant(payload)
            session = DB_SESSION()

            fr = FindingRestaurant(payload['Restaurant_id'],
                                   payload['Location'],
                                   payload['Restaurant_type'],
                                   payload['Delivery_option'],
                                   payload['Open_on_weekends'])

            session.add(fr)  # SQL insert statement
            session.commit()
            session.close()
            logger.debug(f'Stored event "Find Restaurant" with a unique id of {payload["Restaurant_id"]}')

        elif msg["type"] == "wr":  # Change this to your event type
            # Store the event2 (i.e., the payload) to the DB
            # write_review(payload)
            session = DB_SESSION()

            wr = WriteReview(payload['Post_id'],
                             payload['Username'],
                             payload['Rate_no'],
                             payload['Review_description'])

            session.add(wr)
            session.commit()
            session.close()
            logger.debug(f'Stored event "Write Review" with a unique id of {payload["Post_id"]}')

        # Commit the new message as being read
        consumer.commit_offsets()


def get_searched_restaurants(timestamp):
    """ Gets new restaurant records after the timestamp """
    session = DB_SESSION()
    timestamp_datetime = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    readings = session.query(FindingRestaurant).filter(FindingRestaurant.date_created >= timestamp_datetime)
    results_list = []

    for reading in readings:
        results_list.append(reading.to_dict())
        session.close()
        logger.info(f"Query for restaurant search records after {timestamp} returns {len(results_list)} results")
    return results_list, 200


def get_posted_reviews(timestamp):
    """ Gets new reviews after the timestamp """
    session = DB_SESSION()
    timestamp_datetime = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    readings = session.query(WriteReview).filter(WriteReview.date_created >= timestamp_datetime)
    results_list = []

    for reading in readings:
        results_list.append(reading.to_dict())
        session.close()
        logger.info(f"Query for review records after {timestamp} returns {len(results_list)} results")
    return results_list, 200


# def find_restaurant(body):
#     """ Receives a request to find a restaurant """
#
#     session = DB_SESSION()
#
#     bp = FindingRestaurant(body['Restaurant_id'],
#                            body['Location'],
#                            body['Restaurant_type'],
#                            body['Delivery_option'],
#                            body['Open_on_weekends'])
#
#     session.add(bp)  # SQL insert statement
#     session.commit()
#     session.close()
#     logger.debug(f'Stored event "Find Restaurant" with a unique id of {body["Restaurant_id"]}')
#
#     return NoContent, 201
#
#
# def write_review(body):
#     """ Receives a review event """
#
#     session = DB_SESSION()
#
#     hr = WriteReview(body['Post_id'],
#                      body['Username'],
#                      body['Rate_no'],
#                      body['Review_description'])
#
#     session.add(hr)
#     session.commit()
#     session.close()
#     logger.debug(f'Stored event "Write Review" with a unique id of {body["Post_id"]}')
#
#     return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
