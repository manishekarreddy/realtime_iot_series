# "export INFLUXDB_TOKEN=K--7f1I33vqCWrOcMXooQyQcg40_2lChOE44QKv_df3fZD3So5oDWuVx3OPQTYeTmGztHm8myMqP0JDQ_xU-gw=="

from confluent_kafka import Consumer
import json
import os, time
from influxdb_client_3 import InfluxDBClient3, Point

token = "K--7f1I33vqCWrOcMXooQyQcg40_2lChOE44QKv_df3fZD3So5oDWuVx3OPQTYeTmGztHm8myMqP0JDQ_xU-gw=="
org = "691"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

kafka_topic = "sensor_data"


def read_config():
    config = {'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
              'security.protocol': 'SASL_SSL',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': 'N4WWAUWAITQBDKI2',
              'sasl.password': 'lzkYJaU0Yy9PmlpPL5fLO/v0ALxSTYEItcaS7jyioBLhEDhO/VsOuCzGKamZF/Kl',
              'session.timeout.ms': '45000'
              }

    return config


config = read_config()
config["group.id"] = "python-group-1"
config["auto.offset.reset"] = "earliest"

client = InfluxDBClient3(host=host, token=token, org=org)

database = "iot_data"


def save_to_influx(data):
    if "location" in data and data["location"] is not None:
        point = (
            Point("weather")
            .tag("location", data["location"])
            .tag("device_id", data["device_id"])
            .field("temperature", float(data["temperature"]))
            .field("humidity", float(data["humidity"]))

        )

        client.write(database=database, record=point)
        print("writing data")
        time.sleep(1)


# creates a new consumer and subscribes to your topic
consumer = Consumer(config)
consumer.subscribe([kafka_topic])

try:
    while True:
        # consumer polls the topic and prints any incoming messages
        print("running")
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            key = msg.key().decode("utf-8")
            value = msg.value().decode("utf-8")
            print(f"Consumed message from topic {kafka_topic}: key = {key:12} value = {value:12}")
            save_to_influx(json.loads(value))
except KeyboardInterrupt:
    pass
finally:
    # closes the consumer connection
    consumer.close()
