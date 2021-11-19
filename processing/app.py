import os
import json
import yaml
import logging
import datetime
import requests
import connexion
from logging import config
from connexion import NoContent
from flask_cors import CORS, cross_origin
from apscheduler.schedulers.background import BackgroundScheduler

YAML_FILE = "openapi.yaml"

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info(f"App Conf File: {app_conf_file}")
logger.info(f"Log Conf File: {log_conf_file}")

def populate_stats() -> NoContent:
    """ Periodically update stats """
    storage_url = app_config["eventstore"]["url"]
    logger.info("Started Periodic Processing!")

    read_from_json()
    rest_record = get_restaurant_records(storage_url)
    review_record = get_review_records(storage_url)
    calc_stat(rest_record[0], review_record[0], rest_record[1], review_record[1])

    logger.debug("Periodic Processing has ended!")

    return NoContent, 200

def get_stats():
    logger.info("Started statistic request Process!")

    try:
        with open(app_config["datastore"]["filename"], 'r') as file:
            data = json.load(file)
        logger.debug(f"Statistics are: {data}")

    except FileNotFoundError:
        logger.error(f"ERROR [404]: Statistics do not exist!")

    logger.info("The statistic request Process has ended!")

    return data, 200

def get_restaurant_records(s_url):
    event_data = read_from_json()
    # http://localhost:8090/finding-restaurant?start_timestamp="2021-10-14T13:14:50Z"$end_timestamp="2021-11-11T13:14:50Z"
    try:
        headers = {"Content-Type": "application/json"}
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        response = requests.get(f'{s_url}finding-restaurant?start_timestamp={event_data["last_updated"]}'
                                f'&end_timestamp={current_datetime}', headers=headers)
        
        json_data = json.loads(response.text)
        total_events = len(json_data)

        logger.info(
            f'Query for restaurant search records between {event_data["last_updated"]} and {current_datetime} '
            f'returns {total_events} results.')

        if not response.status_code // 100 == 2:
            return logger.debug(
                f'Query for review [ERROR]: Error: Unexpected response [{response.status_code}]: {response}.')

        # print(response.status_code, response.json())

    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        return f"Error: {e}"

    return [json_data, total_events]

def get_review_records(s_url):
    event_data = read_from_json()
    # http://localhost:8090/wrinting-review?start_timestamp="2021-10-14T13:14:50Z"$end_timestamp="2021-11-11T13:14:50Z"
    try:
        headers = {"Content-Type": "application/json"}
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        response = requests.get(f'{s_url}wrinting-review?start_timestamp={event_data["last_updated"]}'
                                f'&end_timestamp={current_datetime}', headers=headers)

        json_data = json.loads(response.text)
        total_events = len(json_data)

        logger.info(
            f'Query for review records between {event_data["last_updated"]} and {current_datetime} '
            f'returns {total_events} results.')

        if not response.status_code // 100 == 2:
            return logger.debug(
                f'Query for restaurant [ERROR]: Error: Unexpected response [{response.status_code}]: {response}.')

        # print(response.status_code, response.json())

    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        return f"Error: {e}"

    return [json_data, total_events]

def read_from_json():
    offset_time = datetime.datetime.now() - datetime.timedelta(minutes=15)
    last_updated = offset_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        with open(app_config["datastore"]["filename"], 'r') as file:
            file = json.load(file)

    except FileNotFoundError:
        sample_data = dict(num_takeouts_available=0,
                           top_reviews=0,
                           all_rest_records=0,
                           all_review_records=0,
                           last_updated=last_updated)

        record_json = json.dumps(sample_data, indent=4, sort_keys=True)

        with open(app_config["datastore"]["filename"], 'w') as file:
            file.write(record_json)
    return file


def write_to_json(takeout, reviews, all_rest_record, all_review_record):
    # print(takeout, reviews)

    current_datetime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    stat_data = dict(num_takeouts_available=takeout,
                     top_reviews=reviews,
                     all_rest_records=all_rest_record,
                     all_review_records=all_review_record,
                     last_updated=current_datetime)

    record_json = json.dumps(stat_data, indent=4, sort_keys=True)

    with open(app_config["datastore"]["filename"], 'w') as file:
        file.write(record_json)

    return file


def calc_stat(restr_data, review_data, rest_len, review_len):
    """ Calculate stats for restaurant searched records filtered by timestamp """
    takeout_counter = 0
    top_review_counter = 0

    all_rest_records = int(rest_len)
    all_review_records = int(review_len)

    for record in restr_data:
        # record = record.decode("utf-8")
        # print(record)
        if record["Delivery_option"] == "takeout":
            takeout_counter += 1

    for record in review_data:
        # record = record.decode("utf-8")
        if record["Rate_no"] == 5:
            top_review_counter += 1

    if takeout_counter > 0 and top_review_counter > 0:
        write_to_json(takeout_counter, top_review_counter, all_rest_records, all_review_records)


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content_Type'
app.add_api(YAML_FILE, strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # run our standalone gevent server
    init_scheduler()
    app.run(port=8100, host="0.0.0.0", use_reloader=False)
