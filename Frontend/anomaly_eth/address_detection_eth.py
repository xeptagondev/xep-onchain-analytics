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

from matplotlib import pyplot as plt
import seaborn as sns

# CHANGE TO YOUR DIRECTORY
os.chdir("C:/Users/Zephyrus G14/Desktop/Capstone Files/xep-onchain-analytics/Frontend")

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
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state= 4103)

'''
indicators = ['hash', 'nonce', 'transaction_index', 'from_address', 'to_address', 'value', 'gas', 'gas_price', 'input', 'receipt_cumulative_gas_used', 
              'receipt_gas_used', 'block_number', 'block_hash',	'year', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']

X_train = pd.DataFrame(X_train, columns = indicators)
X_test = pd.DataFrame(X_test, columns = indicators)
'''

# LOGISTIC REGRESSION
num_transformer = Pipeline(steps=[('scaler', StandardScaler())])
cat_transformer = Pipeline(steps=[('onehot', OneHotEncoder(handle_unknown = 'ignore', drop = 'first'))])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', num_transformer, num_features),
        ('cat', cat_transformer, cat_features)
    ]
)

lr_model = Pipeline(steps=[('preprocessor', preprocessor),
                               ('classifier', LogisticRegression(solver = 'liblinear', penalty = 'l1'))])

lr_model.fit(X_train, y_train)
y_train_lr_pred = lr_model.predict(X_train)
# Training Accuracy
print("Train Accuracy LogR:", accuracy_score(y_train, y_train_lr_pred))

y_test_lr_pred = lr_model.predict(X_test)
# Testing Accuracy
print("Test Accuracy LogR:", accuracy_score(y_test, y_test_lr_pred))

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

# NEURAL NETWORKS
scaler = preprocessing.StandardScaler()
X_train_std = scaler.fit_transform(X_train)
X_test_std = scaler.transform(X_test)

nn_model = keras.Sequential()

# Add an input layer (for tabular data, the input shape matches the number of features)
input_dim = 20 # Define the number of input features
nn_model.add(layers.InputLayer(input_shape=(input_dim,)))

# Add one or more hidden layers
nn_model.add(layers.Dense(128, activation='relu', kernel_regularizer=l2(0.01)))
nn_model.add(layers.Dense(64, activation='relu')) 
nn_model.add(layers.Dense(64, activation='relu'))
nn_model.add(layers.Dropout(0.2))
nn_model.add(layers.Dense(128, activation='relu'))
nn_model.add(layers.Dense(64, activation='relu'))
nn_model.add(layers.Dense(32, activation='relu'))
nn_model.add(layers.Dense(64, activation='relu'))
nn_model.add(layers.Dense(64, activation='relu'))

# Add the output layer with a sigmoid activation function (for binary classification)
nn_model.add(layers.Dense(1, activation='sigmoid'))

adam = keras.optimizers.Adam(learning_rate = 0.0001)

# Compile the model
nn_model.compile(optimizer=adam,
              loss='binary_crossentropy',  # Binary cross-entropy loss for binary classification
              metrics=['accuracy', Precision(), Recall()])

nn_model.fit(X_train_std, y_train, epochs=150)

y_train_nn_pred = nn_model.predict(X_train_std)

threshold = 0.5
y_train_nn_pred = np.where(y_train_nn_pred > threshold, 1, 0)

# Training Accuracy
print("Train Accuracy NN:", accuracy_score(y_train, y_train_nn_pred))

y_test_nn_pred = nn_model.predict(X_test_std)
y_test_nn_pred = np.where(y_test_nn_pred > threshold, 1, 0)

# Testing Accuracy
print("Test Accuracy NN:", accuracy_score(y_test, y_test_nn_pred))


# RANDOM FOREST
rf_model = RandomForestClassifier(class_weight = "balanced")
rf_model.fit(X_train, y_train)

y_train_rf_pred = rf_model.predict(X_train)
# Training Accuracy
print("Train Accuracy RF:", accuracy_score(y_train, y_train_rf_pred))

y_test_rf_pred = rf_model.predict(X_test)
# Testing Accuracy
print("Test Accuracy RF:", accuracy_score(y_test, y_test_rf_pred))

# Pickle models
pickle_classifier_string_lr = pickle.dumps(lr_model)
pickle_classifier_string_xgb = pickle.dumps(xgb_model)
pickle_classifier_string_nn = pickle.dumps(nn_model)
pickle_classifier_string_rf = pickle.dumps(rf_model)

classifier = ["logr", "xgb", "nn", "rf"]
models = [pickle_classifier_string_lr, pickle_classifier_string_xgb, pickle_classifier_string_nn, pickle_classifier_string_rf]
y_pred_train_model = [y_train_lr_pred, y_train_xgb_pred, y_train_nn_pred, y_train_rf_pred]
y_pred_test_model = [y_test_lr_pred, y_test_xgb_pred, y_test_nn_pred, y_test_rf_pred]

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
