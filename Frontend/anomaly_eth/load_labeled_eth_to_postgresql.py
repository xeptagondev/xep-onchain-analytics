import psycopg2
import pandas as pd
import requests
import json
import io
import os

# Initializes the table to fetch anomalous labels from bitcoin_abuse database into postgresql

# CHANGE TO YOUR DIRECTORY
os.chdir("/Users/jasminewang/Desktop/Capstone Files/xep-onchain-analytics/Frontend/")


# Database configurations
with open("extract/config.json") as config_file:
    config = json.load(config_file)

# Connecting to Database
conn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# To execute queries and retrieve data
cursor = conn.cursor()

df = pd.read_csv('anomaly_eth/Dataset.csv')

cursor.execute("DROP TABLE IF EXISTS eth_labeled_data")
cursor.execute("""CREATE TABLE eth_labeled_data (
                        hash text, 
                        nonce bigint, 
                        transaction_index bigint, 
                        from_address text, 
                        to_address text, 
                        value double precision, 
                        gas bigint, 
                        gas_price bigint, 
                        input text, 
                        receipt_cumulative_gas_used bigint, 
                        receipt_gas_used bigint, 
                        block_timestamp text, 
                        block_number bigint, 
                        block_hash text, 
                        from_scam int, 
                        to_scam int, 
                        from_category text, 
                        to_category text
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
                            block_timestamp, 
                            block_number, 
                            block_hash, 
                            from_scam, 
                            to_scam, 
                            from_category, 
                            to_category
               )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
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
            row.block_timestamp, 
            row.block_number, 
            row.block_hash, 
            row.from_scam, 
            row.to_scam, 
            row.from_category, 
            row.to_category
            ))
    conn.commit()

cursor.close()
conn.close()