import pandas as pd
import duckdb as ddb
import psycopg2
from sqlalchemy import create_engine
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
import duckdb as ddb
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import VotingClassifier
from sklearn import preprocessing
import pickle
import codecs
import json
import os
import numpy as np
import warnings

warnings.filterwarnings("ignore")

# CHANGE TO YOUR DIRECTORY
os.chdir("xep-onchain-analytics/Frontend")

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

query = "SELECT * FROM eth_labeled_data"
df = pd.read_sql_query(query, con=engine)
df_copy = df.copy()

# Drop from_address and to_address so that models can be generalized
df =  df.drop(columns = ['from_address', 'to_address'])

select_query = """
SELECT * FROM anomaly_models_eth
"""

cat_features = ['hash', 'input', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']
num_features = ['nonce', 'transaction_index', 'value', 'gas', 'gas_price', 'receipt_cumulative_gas_used', 'receipt_gas_used', 'block_number', 'year']

# Basic preprocessing for Label Encoder

le = preprocessing.LabelEncoder()
for i in df.columns:
    if i in cat_features:
        le.fit(df[i])
        df[i]=le.transform(df[i])

#ASSIGNS NUMBER TO EVERY LABEL
for i in df.columns:
    le.fit(df[i])
    df[i]=le.transform(df[i])

models = cursor.execute(select_query)
models = cursor.fetchall()

logr_model = pickle.loads(codecs.decode(models[0][2].encode(), "base64"))
xgb_model = pickle.loads(codecs.decode(models[1][2].encode(), "base64"))
rf_model = pickle.loads(codecs.decode(models[2][2].encode(), "base64"))
ensemble_model = pickle.loads(codecs.decode(models[3][2].encode(), "base64"))

indicators = ['hash', 'nonce', 'transaction_index', 'value', 'gas', 'gas_price', 'input', 'receipt_cumulative_gas_used', 
              'receipt_gas_used', 'block_number', 'block_hash',	'year', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']

# Merging predicted result back to dataframe
X_general = df[indicators]
y_logr_pred = logr_model.predict(X_general)
y_xgb_pred = xgb_model.predict(X_general)
y_rf_pred = rf_model.predict(X_general)
y_ensemble_pred = ensemble_model.predict(X_general)

outputs = pd.DataFrame()

outputs['y_logr_pred'] = y_logr_pred
outputs['y_xgb_pred'] = y_xgb_pred
outputs['y_rf_pred'] = y_rf_pred
outputs['y_ensemble_pred'] = y_ensemble_pred

df_output = pd.merge(df_copy, outputs, how = 'left', left_index = True, right_index = True)
df_output.fillna(0, inplace = True)
df_output = df_output.loc[df_output['is_fraud'].ne(0) | df_output['y_logr_pred'].ne(0) | df_output['y_xgb_pred'].ne(0) | df_output['y_rf_pred'].ne(0) | df_output['y_ensemble_pred'].ne(0)]

with engine.connect() as conn:
    print(bool(conn)) # <- just to keep track of the process
    
    df_output.to_sql(name='anomaly_predictions_eth', con=engine, if_exists='replace', index=True)
    print("end") # <- just to keep track of the process

cursor.close()
conn.close()
psqlconn.close()