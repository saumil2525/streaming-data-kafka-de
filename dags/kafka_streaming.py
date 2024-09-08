import logging
import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from datetime import datetime

# Dag default args
default_argv = {"owner": "sam", "start_date": datetime(2023, 11, 3)}

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_data() -> dict:
    import requests

    _URL = "https://randomuser.me/api/"
    response = requests.get(url=_URL)
    response = response.json()
    response = response["results"][0]
    return response


def format_data(response: dict) -> dict:
    data = {}
    data["first_name"] = response["name"]["first"]
    data["last_name"] = response["name"]["last"]
    data["gender"] = response["gender"]
    data["address"] = (
        f"{str(response['location']['street']['number'])} {response['location']['street']['name']}, "
        f"{response['location']['city']}, {response['location']['state']}, {response['location']['country']}"
    )
    data["postcode"] = response["location"]["postcode"]
    data["email"] = response["email"]
    data["username"] = response["login"]["username"]
    data["dob"] = response["dob"]["date"]
    data["registered_date"] = response["registered"]["date"]
    data["phone"] = response["phone"]
    data["picture"] = response["picture"]["medium"]
    return data


def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def stream_data():

    # set producer config
    # "bootstrap.servers": ""broker:29092"" with airflow
    # "bootstrap.servers": "localhost:9092" for lwithour airflow
    conf = {
        "bootstrap.servers": "broker:29092",
        "client.id": "user-stream-producer",
    }

    producer = Producer(conf)
    current_time = time.time()
    while True:

        if time.time() > current_time + 120:  # 2 mins
            break
        try:
            response = get_data()
            formatted_data = format_data(response)
            message_value = json.dumps(formatted_data).encode("utf-8")
            producer.produce(
                "user-stream-producer",
                value=message_value,
                callback=delivery_report,  # qa E501
            )
        except Exception as e:
            logging.error(f" error msg: {e}")
            continue


with DAG(
    "automation", default_args=default_argv, schedule_interval="@daily", catchup=False
) as dag:

    stream_task = PythonOperator(task_id="stream_from_api", python_callable=stream_data)
