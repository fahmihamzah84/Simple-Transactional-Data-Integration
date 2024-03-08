from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os 

client = bigquery.Client()

credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

table_id = f"{os.environ["PROJECT"]}.{os.environ["DATASET"]}.{os.environ["TABLE"]}"
dataset_ref = client.dataset(os.environ["DATASET"], project=os.environ["PROJECT"])
table_ref = dataset_ref.table(os.environ["TABLE"])
table = client.get_table(table_id)


def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "transaction.bank.avro.consumer.2",
                       "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["transaction.bank"])

    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                # INSERT STREAM TO BIGQUERY
                client.insert_rows(table, [message.value()])
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()

