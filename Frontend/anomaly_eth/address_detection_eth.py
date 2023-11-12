import pandas as pd
import duckdb as ddb
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
import duckdb as ddb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import pickle
import codecs
import json
import os
import numpy as np
import xgboost as xgb
from sklearn import preprocessing
import tensorflow as tf
from tensorflow.keras import layers
import keras
from keras.models import Sequential
from keras.layers import Dense
from keras.metrics import Precision, Recall
from tensorflow.keras.regularizers import l2
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import VotingClassifier

from matplotlib import pyplot as plt
import seaborn as sns

# CHANGE TO YOUR DIRECTORY
os.chdir("xep-onchain-analytics/Frontend")

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

query = "SELECT * FROM eth_labeled_data"
df = pd.read_sql_query(query, con=engine)
df_copy = df.copy()

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

y = df_copy.pop('is_fraud')
X = df_copy
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state= 4103)

# LOGISTIC REGRESSION
num_transformer = Pipeline(steps=[('scaler', StandardScaler())])
cat_transformer = Pipeline(steps=[('onehot', OneHotEncoder(handle_unknown = 'ignore', drop = 'first'))])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', num_transformer, num_features),
        ('cat', cat_transformer, cat_features)
    ]
)

logr_model = Pipeline(steps=[('preprocessor', preprocessor),
                               ('classifier', LogisticRegression(solver = 'liblinear', penalty = 'l1'))])
logr_model.fit(X_train, y_train)
y_train_logr_pred = logr_model.predict(X_train)

# Training Accuracy
print("Train Accuracy LogR:", accuracy_score(y_train, y_train_logr_pred))

y_test_logr_pred = logr_model.predict(X_test)
# Testing Accuracy
print("Test Accuracy LogR:", accuracy_score(y_test, y_test_logr_pred))

# XGB
params = {'subsample': 0.8, 'reg_lambda': 0.8, 'min_child_weight': 1, 'max_depth': 14, 'learning_rate': 0.03, 'gamma': 1.5, 'colsample_bytree': 0.8}
xgb_model = XGBClassifier(**params)
xgb_model.fit(X_train, y_train)

# running on Train and Test
y_train_xgb_pred = xgb_model.predict(X_train)
print("Train Accuracy XGB:", accuracy_score(y_train, y_train_xgb_pred))

y_test_xgb_pred = xgb_model.predict(X_test)
print("Test Accuracy XGB:", accuracy_score(y_test, y_test_xgb_pred))

# RANDOM FOREST
rf_model = RandomForestClassifier(class_weight = "balanced")
rf_model.fit(X_train, y_train)

y_train_rf_pred = rf_model.predict(X_train)
# Training Accuracy
print("Train Accuracy RF:", accuracy_score(y_train, y_train_rf_pred))

y_test_rf_pred = rf_model.predict(X_test)
# Testing Accuracy
print("Test Accuracy RF:", accuracy_score(y_test, y_test_rf_pred))

# ENSEMBLE
ensemble_model = VotingClassifier(estimators=[
    ('logr', logr_model),
    ('xgb', xgb_model),
    ('rf', rf_model)
], voting='hard')  # 'hard' for majority voting

ensemble_model.fit(X_train, y_train)

# Predictions
y_train_ensemble_pred = ensemble_model.predict(X_train)
y_test_ensemble_pred = ensemble_model.predict(X_test)

print("Train Accuracy ENSEMBLE:", accuracy_score(y_train, y_train_ensemble_pred))
print("Test Accuracy ENSEMBLE:", accuracy_score(y_test, y_test_ensemble_pred))

# Pickle models
pickle_classifier_string_logr = pickle.dumps(logr_model)
pickle_classifier_string_xgb = pickle.dumps(xgb_model)
pickle_classifier_string_rf = pickle.dumps(rf_model)
pickle_classifier_string_ensemble = pickle.dumps(ensemble_model)

classifier = ["logr", "xgb", "rf", "ensemble"]
models = [pickle_classifier_string_logr, pickle_classifier_string_xgb, pickle_classifier_string_rf, pickle_classifier_string_ensemble]
y_pred_train_model = [y_train_logr_pred, y_train_xgb_pred, y_train_rf_pred, y_train_ensemble_pred]
y_pred_test_model = [y_test_logr_pred, y_test_xgb_pred, y_test_rf_pred, y_test_ensemble_pred]

print(bool(dbConnection)) # <- just to keep track of the process
df = pd.DataFrame(columns=['class', 'model'])
df.to_sql(name='anomaly_models_eth', con=engine, if_exists='replace', index=True, index_label= "id")

for i in range(0, 4):
    print(i)
    pickled = codecs.encode(models[i], "base64").decode()

    query = "INSERT INTO anomaly_models_eth VALUES (%s, %s, %s)"
    cursor.execute(query, (i, classifier[i], pickled))
    psqlconn.commit()

    print("commit model")


df = pd.DataFrame(columns=['class', 'train_acc', 'train_precision', 'train_recall', 'train_f1score', 'test_acc', 'test_precision', 'test_recall', 'test_f1score'])

df.to_sql(name='anomaly_results_eth', con=engine, if_exists='replace', index=True, index_label= "id")

for i in range(0, 4):
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
