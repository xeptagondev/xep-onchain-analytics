import pandas as pd
import duckdb as ddb
import psycopg2
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
import duckdb as ddb
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import pickle
import codecs
import json
import os
import numpy as np
import xgboost as xgb
from sklearn import preprocessing

# CHANGE TO YOUR DIRECTORY
os.chdir("/Users/jasminewang/Desktop/Capstone Files/xep-onchain-analytics/Frontend/")

# Database configurations
with open("extract/config.json") as config_file:
    config = json.load(config_file)

df = pd.read_csv('anomaly_eth/Dataset.csv')

# Connecting to Database
engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# To execute queries and retrieve data
cursor = psqlconn.cursor()
dbConnection = engine.connect()

df_copy = df.copy()
df_copy['is_fraud'] = np.where((df_copy['from_scam'] == 1) | (df_copy['to_scam'] == 1), 1, 0)
df_copy = df_copy.drop(columns = ['from_category', 'to_category', 'from_scam', 'to_scam'])

# Process block_timestamp properly
# Create more features from "timestamp" as just the timestamp itself is quite useless, model will just memorise it as a string

# Some of the timestamps not in proper format so this code standardizes it
proper_time_end = " UTC"

for index, row in df_copy.iterrows():
    last_six_char = row['block_timestamp'][-6:]
    if last_six_char == '+00:00':
        df_copy.at[index, 'block_timestamp'] = row['block_timestamp'][:-6] + proper_time_end

df_copy['block_timestamp'] = pd.to_datetime(df_copy['block_timestamp'   ])

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

cat_features = ['hash', 'from_address', 'to_address', 'input', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']
num_features = ['nonce', 'transaction_index', 'value', 'gas', 'gas_price', 'receipt_cumulative_gas_used', 'receipt_gas_used', 'block_number', 'year']

# Basic preprocessing for Label Encoder

le = preprocessing.LabelEncoder()
for i in df_copy.columns:
    if i in cat_features:
        le.fit(df_copy[i])
        df_copy[i]=le.transform(df_copy[i])

#ASSIGNS NUMBER TO EVERY LABEL
for i in df_copy.columns:
    le.fit(df_copy[i])
    df_copy[i]=le.transform(df_copy[i])

df_copy.to_csv('anomaly_eth/Preprocessed_dataset.csv', index = False)

y = df_copy.pop('is_fraud')
X = df_copy
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state= 4103)

indicators = ['hash', 'nonce', 'transaction_index', 'from_address', 'to_address', 'value', 'gas', 'gas_price', 'input', 'receipt_cumulative_gas_used', 
              'receipt_gas_used', 'block_number', 'block_hash',	'year', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']

X_train = pd.DataFrame(X_train, columns = indicators)
X_test = pd.DataFrame(X_test, columns = indicators)

# XGB
params = {'subsample': 0.8, 'reg_lambda': 0.8, 'min_child_weight': 1, 'max_depth': 14, 'learning_rate': 0.03, 'gamma': 1.5, 'colsample_bytree': 0.8}
xgb_model = XGBClassifier(**params)

# Training XGBoost model
xgb_model.fit(X_train, y_train)

# running on Train and Test
y_train_xgb_pred = xgb_model.predict(X_train)
print("Train Accuracy XGB:", accuracy_score(y_train, y_train_xgb_pred))

y_test_xgb_pred = xgb_model.predict(X_test)
print("Test Accuracy XGB:", accuracy_score(y_test, y_test_xgb_pred))

# Pickle models

pickle_classifier_string_xgb = pickle.dumps(xgb_model)

classifier = ["xgb"]
models = [pickle_classifier_string_xgb]
y_pred_train_model = [y_train_xgb_pred]
y_pred_test_model = [y_test_xgb_pred]

print(bool(dbConnection)) # <- just to keep track of the process
df = pd.DataFrame(columns=['class', 'model'])
df.to_sql(name='anomaly_models_eth', con=engine, if_exists='replace', index=True, index_label= "id")

for i in range(0, 1):
    print(i)
    pickled = codecs.encode(models[i], "base64").decode()

    query = "INSERT INTO anomaly_models_eth VALUES (%s, %s, %s)"
    cursor.execute(query, (i, classifier[i], pickled))
    psqlconn.commit()

    print("commit model")


df = pd.DataFrame(columns=['class', 'train_acc', 'train_precision', 'train_recall', 'train_f1score', 'test_acc', 'test_precision', 'test_recall', 'test_f1score'])

df.to_sql(name='anomaly_results_eth', con=engine, if_exists='replace', index=True, index_label= "id")

for i in range(0,1):
    print(i)
    y_pred_train = y_pred_train_model[i]
    y_pred_test = y_pred_test_model[i]

    train_accuracy = accuracy_score(y_train, y_pred_train)
    train_precision = precision_score(y_train, y_pred_train)
    train_recall = recall_score(y_train, y_pred_train)
    train_f1_score = f1_score(y_train, y_pred_train)

    test_accuracy = accuracy_score(y_test, y_pred_test)
    test_precision = precision_score(y_test, y_pred_test)
    test_recall = recall_score(y_test, y_pred_test)
    test_f1_score = f1_score(y_test, y_pred_test)

    query = "INSERT INTO anomaly_results_eth VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (i, classifier[i], train_accuracy, train_precision, train_recall, train_f1_score, test_accuracy, test_precision, test_recall, test_f1_score))
    psqlconn.commit()

cursor.close()
print("end") # <- just to keep track of the process


dbConnection.close()
psqlconn.close()
