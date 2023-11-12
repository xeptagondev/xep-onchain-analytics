import psycopg2
import pandas as pd
import requests
import json
import io
import os
from sqlalchemy import create_engine

# Initializes the table to fetch anomalous labels from bitcoin_abuse database into postgresql

# CHANGE TO YOUR DIRECTORY
os.chdir("xep-onchain-analytics/Frontend")


# Database configurations
with open("extract/config.json") as config_file:
    config = json.load(config_file)

df = pd.read_csv('anomaly_eth/Dataset.csv')

# Connecting to Database
conn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# To execute queries and retrieve data
cursor = conn.cursor()

df_copy = df.copy()
df_copy['is_fraud'] = (df_copy['to_scam'] | df_copy['from_scam']).astype(int)
df_copy = df_copy.drop(columns = ['from_category', 'to_category', 'from_scam', 'to_scam'])

# Process block_timestamp properly
# Create more features from "timestamp" as just the timestamp itself is quite useless, model will just memorise it as a string

# Some of the timestamps not in proper format so this code standardizes it
proper_time_end = " UTC"

for index, row in df_copy.iterrows():
    last_six_char = row['block_timestamp'][-6:]
    if last_six_char == '+00:00':
        df_copy.at[index, 'block_timestamp'] = row['block_timestamp'][:-6] + proper_time_end

df_copy['block_timestamp'] = pd.to_datetime(df_copy['block_timestamp'])

df_copy['year'] =  df_copy['block_timestamp'].dt.year
df_copy['month'] = df_copy['block_timestamp'].dt.month
df_copy['day_of_the_month'] = df_copy['block_timestamp'].dt.day
df_copy['day_name'] = df_copy['block_timestamp'].dt.strftime('%A')
df_copy['hour'] = df_copy['block_timestamp'].dt.hour

# Define daypart based on the hour
df_copy['daypart'] = df_copy['block_timestamp'].apply(lambda x: "Morning" if 5 <= x.hour < 12 else
                                              "Afternoon" if 12 <= x.hour < 17 else
                                              "Evening" if 17 <= x.hour < 21 else
                                              "Night")

# Define weekend flag (1 if weekend, else 0)
df_copy['weekend_flag'] = df_copy['block_timestamp'].apply(lambda x: 1 if x.weekday() >= 5 else 0)

# Drop "block_timestamp"
df_copy = df_copy.drop(columns = ['block_timestamp'])

cursor.execute("DROP TABLE IF EXISTS eth_labeled_data")
cursor.execute("""CREATE TABLE eth_labeled_data (
                        hash TEXT, 
                        nonce BIGINT, 
                        transaction_index BIGINT, 
                        from_address TEXT, 
                        to_address TEXT, 
                        value DOUBLE PRECISION, 
                        gas BIGINT, 
                        gas_price BIGINT, 
                        input TEXT, 
                        receipt_cumulative_gas_used BIGINT, 
                        receipt_gas_used BIGINT, 
                        block_number BIGINT, 
                        block_hash TEXT, 
                        is_fraud INT,
                        year INT,
                        month INT,
                        day_of_the_month INT,
                        day_name TEXT,
                        hour INT,
                        daypart TEXT,
                        weekend_flag INT
            )""")

for row in df_copy.itertuples():
    cursor.execute("""INSERT INTO eth_labeled_data (
                        hash, 
                        nonce, 
                        transaction_index, 
                        from_address, 
                        to_address, 
                        value, 
                        gas, 
                        gas_price, 
                        input, 
                        receipt_cumulative_gas_used, 
                        receipt_gas_used, 
                        block_number, 
                        block_hash, 
                        is_fraud,
                        year,
                        month,
                        day_of_the_month,
                        day_name,
                        hour,
                        daypart,
                        weekend_flag
               )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (row.hash,
            row.nonce, 
            row.transaction_index, 
            row.from_address, 
            row.to_address, 
            row.value, 
            row.gas, 
            row.gas_price, 
            row.input, 
            row.receipt_cumulative_gas_used, 
            row.receipt_gas_used, 
            row.block_number, 
            row.block_hash, 
            row.is_fraud, 
            row.year, 
            row.month, 
            row.day_of_the_month,
            row.day_name,
            row.hour,
            row.daypart,
            row.weekend_flag
            ))
    conn.commit()

cursor.close()
conn.close()