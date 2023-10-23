import pandas as pd
import duckdb as ddb
import psycopg2
from sqlalchemy import create_engine
from xgboost import XGBClassifier
import duckdb as ddb
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
import pickle
import codecs
import json
import os
import numpy as np

# CHANGE TO YOUR DIRECTORY
os.chdir("/Users/jasminewang/Desktop/Capstone Files/xep-onchain-analytics/Frontend/")

# Database configurations
with open("extract/config.json") as config_file:
    config = json.load(config_file)



# Connect to Database
engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# To execute queries and retrieve data
cursor = psqlconn.cursor()
dbConnection = engine.connect()

df = pd.read_csv('anomaly_eth/Preprocessed_dataset.csv')

select_query = """
SELECT * FROM anomaly_models_eth
"""

models = cursor.execute(select_query)
models = cursor.fetchall()

xgb_model = pickle.loads(codecs.decode(models[0][2].encode(), "base64"))

indicators = ['hash', 'nonce', 'transaction_index', 'from_address', 'to_address', 'value', 'gas', 'gas_price', 'input', 'receipt_cumulative_gas_used', 
              'receipt_gas_used', 'block_number', 'block_hash',	'year', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']

# Merging predicted result back to dataframe
X_general = df[indicators]
y_xgb_pred = xgb_model.predict(X_general)

outputs = pd.DataFrame()

outputs['y_xgb_pred'] = y_xgb_pred

df_output = pd.merge(df, outputs, how = 'left', left_index = True, right_index = True)
df_output.fillna(0, inplace = True)
df_output = df_output.loc[df_output['is_fraud'].ne(0) | df_output['y_xgb_pred'].ne(0)]

with engine.connect() as conn:
    print(bool(conn)) # <- just to keep track of the process
    
    df_output.to_sql(name='anomaly_predictions_eth', con=engine, if_exists='replace', index=True)
    print("end") # <- just to keep track of the process

cursor.close()
conn.close()
psqlconn.close()