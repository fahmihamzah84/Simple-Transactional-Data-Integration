from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
import re
import pandas as pd
import random
from datetime import datetime, timedelta


def load_avro_schema_from_file():
    key_schema = avro.load("transaction_key.avsc")
    value_schema = avro.load("transaction_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    data = pd.read_csv("../nasabah.csv", delimiter=";")
    sample_data = data.to_dict(orient="records")
    while True:
        sender = random.choice(sample_data)
        receiver = random.choice(sample_data)
        while receiver["idUser"] == sender["idUser"]:
                receiver = random.choice(sample_data)
        transaction_Id = random.randint(100000, 999999)
        date = datetime(2024, 4, 1) + timedelta(hours=random.randint(0, 730), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        date_timestamp = int(date.timestamp() * 1000)
        amount = random.randint(10000, 50000000)
        namaSender = data[data["idUser"] == sender["idUser"]]["fullName"].iloc[0]
        namaReceiver = data[data["idUser"] == receiver["idUser"]]["fullName"].iloc[0]
        key = {"transaction_Id": transaction_Id}
        value = {
                    "transaction_Id": transaction_Id,
                    "date": date_timestamp,
                    "id_sender": sender["idUser"],
                    "namaSender": namaSender,
                    "id_receiver": receiver["idUser"],
                    "namaReceiver": namaReceiver,
                    "amount": amount
                }
        try:
            producer.produce(topic='transaction.bank', key=key, value=value)

        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")


        producer.flush()
        sleep(1)

if __name__ == "__main__":
    send_record()
