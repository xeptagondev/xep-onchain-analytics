import psycopg2
import pandas as pd
import requests
import json
import io
import os
from sqlalchemy import create_engine

# Database configurations
with open('../../config.json') as config_file:
    config = json.load(config_file)

df = pd.read_csv('Dataset.csv')

# Connecting to Database
conn = psycopg2.connect(database = config['postgre_extract']['database'],
                            host = config['postgre_extract']['host'],
                            user = config['postgre_extract']['user'],
                            password = config['postgre_extract']['password'],
                            port = config['postgre_extract']['port'])

# To execute queries and retrieve data
cursor = conn.cursor()

df = df.drop(columns = ['from_category', 'to_category'])
df['is_fraud'] = (df['to_scam'] | df['from_scam']).astype(int)

# Process block_timestamp
proper_time_end = " UTC"

for index, row in df.iterrows():
    last_six_char = row['block_timestamp'][-6:]
    if last_six_char == '+00:00':
        df.at[index, 'block_timestamp'] = row['block_timestamp'][:-6] + proper_time_end
        
df['block_timestamp'] = pd.to_datetime(df['block_timestamp'])

df['block_timestamp'] = df['block_timestamp'].dt.tz_localize(None)

df['year'] = df['block_timestamp'].dt.year
df['month'] = df['block_timestamp'].dt.month
df['week_of_year'] = df['block_timestamp'].dt.isocalendar().week
df['day_of_the_month'] = df['block_timestamp'].dt.day
df['day_name'] = df['block_timestamp'].dt.strftime('%A')
df['hour'] = df['block_timestamp'].dt.hour

# Define daypart based on the hour
df['daypart'] = df['block_timestamp'].apply(lambda x: "Morning" if 5 <= x.hour < 12 else
                                              "Afternoon" if 12 <= x.hour < 17 else
                                              "Evening" if 17 <= x.hour < 21 else
                                              "Night")

# Define weekend flag (1 if weekend, else 0)
df['weekend_flag'] = df['block_timestamp'].apply(lambda x: 1 if x.weekday() >= 5 else 0)

df = df.drop(columns=['block_timestamp'])

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

for row in df.itertuples():
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