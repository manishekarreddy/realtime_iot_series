# python 3.11

import random
import time
from paho.mqtt import client as mqtt_client
from confluent_kafka import Producer
import json

broker = 'p01c1def.ala.dedicated.aws.emqxcloud.com'
port = 1883
mqtt_topic = "sensor/weatherdata"
# Generate a Client ID with the subscribe prefix.


client_id = f'subscribe-{random.randint(0, 100)}'
username = 'mani'
password = 'pass'

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

def connect_mqtt() -> mqtt_client:
    print("connecting to mqtt")
    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)

    print("Attempting connection")
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client, producer: Producer):
    def on_message(client, userdata, msg):
        # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic at `{time.localtime()}`")
        data = json.loads(msg.payload.decode())
        print(data)
        print("producing messages to kafka topics")
        producer.produce(kafka_topic, key="key", value=msg.payload.decode())

    client.subscribe(mqtt_topic)
    client.on_message = on_message


def run():
    print("run method")
    client = connect_mqtt()
    producer = Producer(read_config())
    subscribe(client, producer)
    client.loop_forever()


if __name__ == '__main__':
    run()
