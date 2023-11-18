import pandas as pd
import duckdb as ddb
import psycopg2
from sqlalchemy import create_engine
import duckdb as ddb
import json
import os

# Labels the daily bitcoin transactions as illicit with data from postgresql fetched from the etl pipeline

# Database configurations
with open('../../config.json') as config_file:
    config = json.load(config_file)

# Connecting to in-memory temporary database
conn_ddb = ddb.connect(config['ddb']['database'])

query = """
    SELECT * 
    FROM TEST_INPUT
    """
df_bc_in = conn_ddb.execute(query).fetchdf()

query = """
    SELECT * 
    FROM TEST_TRANSACTIONS
    """
df_transactions = conn_ddb.execute(query).fetchdf()

# Connect to Postgres database
engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database = config['postgre_extract']['database'],
                            host = config['postgre_extract']['host'],
                            user = config['postgre_extract']['user'],
                            password = config['postgre_extract']['password'],
                            port = config['postgre_extract']['port'])

# To execute queries and retrieve data
cursor = psqlconn.cursor()
dbConnection = engine.connect()

df_bc_in = df_bc_in.rename(columns={"recipient": "account", "transaction_hash": "hash"})
df_transactions = df_transactions.rename(columns={"transaction_hash": "hash"})
pd.set_option('display.max_columns', None)

# Read illicit labels from postgres
select_sql = "SELECT * FROM ILLICIT_LABEL WHERE LABEL = 1"
df_abuse = pd.read_sql(select_sql, psqlconn)

df_in_abuse = pd.merge(df_bc_in, df_abuse, on='account', how='left')
df = pd.merge(df_in_abuse, df_transactions, on='hash', how='inner')
df.fillna(0, inplace=True)

conn_ddb.execute("CREATE OR REPLACE TABLE anomaly_df AS SELECT * FROM df")
conn_ddb.execute("INSERT INTO anomaly_df SELECT * FROM df")

cursor.close()
conn_ddb.close()
psqlconn.close()