import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd
import pyspark.pandas as ps
import argparse
import os

def generate_random_transactions(spark, sample_data, num_transactions):
    transactions = []
    sample_data = sample_data.collect()
    for dates in range(1, 31):
        for _ in range(num_transactions):
            sender = random.choice(sample_data)
            receiver = random.choice(sample_data)
            while receiver["idUser"] == sender["idUser"]:
                receiver = random.choice(sample_data)
            
            transaction_id = random.randint(100000, 9999999)
            date = datetime(2024, 3, dates) + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
            amount = random.randint(10000, 50000000)
            transactions.append({
                "transaction_Id": transaction_id,
                "date": date.strftime("%Y-%m-%d %H:%M:%S"),
                "id_sender": sender["idUser"],
                "id_receiver": receiver["idUser"],
                "amount": amount
            })
    df = spark.createDataFrame(transactions)
    pddf = df.pandas_api()
    date = pd.to_datetime(pddf['date'].iloc[0])
    pddf.to_csv(f"{date.strftime('%Y%m%d')}_transaction", sep=";", index=False, num_files=1)

def separate_file(dir):
    for filename in os.listdir(dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(dir, filename)
            df = pd.read_csv(file_path, sep=";")
            df['date'] = pd.to_datetime(df['date'])
            grouped = df.groupby(df['date'].dt.date)
            for date, group in grouped:
                output_filename = f"~/etlTools/assignment/gcs/{date}_transaction.csv"
                group.to_csv(output_filename, index=False, sep=";")
    
    for filename in os.listdir(dir):
        if filename.startswith("part-00000"):
            file_path = os.path.join(dir, filename)
            os.remove(file_path)

def main():
    parser = argparse.ArgumentParser(description="Randomize data from a cv")
    parser.add_argument("--num", type=str, help="number of randomized data", default=1000)
    args = parser.parse_args()
    spark = SparkSession.builder \
        .appName("myProject") \
        .getOrCreate()

    sample_data_df = spark.read.option("delimiter", ";").option("header", True).csv("nasabah.csv")


    generate_random_transactions(spark, sample_data_df, args.num)
    separate_file("20240301_transaction/")
    spark.stop()

if __name__ == "__main__":
    main()