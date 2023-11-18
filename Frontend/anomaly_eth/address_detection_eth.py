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
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, LabelEncoder, MinMaxScaler, OneHotEncoder, OrdinalEncoder
from sklearn.ensemble import VotingClassifier
from sklearn.feature_selection import SelectFromModel

from matplotlib import pyplot as plt
import seaborn as sns

# CHANGE TO YOUR DIRECTORY
os.chdir("xep-onchain-analytics/Frontend")

# Database configurations
with open("extract/config.json") as config_file:
    config = json.load(config_file)

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

# Store model results
results = pd.DataFrame(columns=['class', 'test_acc', 'test_precision', 'test_recall', 'test_f1score'])
results.to_sql(name='anomaly_results_eth', con=engine, if_exists='replace', index=True, index_label= "id")
results_query = "INSERT INTO anomaly_results_eth VALUES (%s, %s, %s, %s, %s, %s)"

# Store model predictions
indicators = ['hash', 'nonce', 'transaction_index', 'value', 'gas', 'gas_price', 'input', 'receipt_cumulative_gas_used', 
              'receipt_gas_used', 'block_number', 'block_hash',	'year', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag']
outputs = pd.DataFrame()

query = "SELECT * FROM eth_labeled_data"
df = pd.read_sql_query(query, con=engine)

df_copy = df.copy(deep=True)

# Drop from_address and to_address so that models can be generalized
df =  df.drop(columns = ['from_address', 'to_address'])

cat_features = ['hash', 'input', 'month', 'day_of_the_month', 'day_name', 'hour', 'daypart', 'weekend_flag', 'block_hash']
num_features = ['nonce', 'transaction_index', 'value', 'gas', 'gas_price', 'receipt_cumulative_gas_used', 'receipt_gas_used', 'block_number', 'year']

# Logistic Regression Model
lr_data = df.copy(deep=True)
num_transformer = Pipeline(steps=[('scaler', StandardScaler())])
cat_transformer = Pipeline(steps=[('onehot', OneHotEncoder(handle_unknown = 'ignore', drop = 'first'))])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', num_transformer, num_features),
        ('cat', cat_transformer, cat_features)
    ]
)

X = lr_data.drop(['is_fraud'], axis=1)
y = lr_data['is_fraud']

# Split into train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 4103)
logr_model = Pipeline(steps=[('preprocessor', preprocessor),
                               ('classifier', LogisticRegression(solver = 'liblinear', penalty = 'l1'))])

logr_model.fit(X_train, y_train)

# Make predictions on test data
y_pred = logr_model.predict(X_test)

# Evaluation metrics
test_accuracy = accuracy_score(y_test, y_pred)
test_precision = precision_score(y_test, y_pred)
test_recall = recall_score(y_test, y_pred)
test_f1_score = f1_score(y_test, y_pred)

cursor.execute(results_query, (0, 'logr', test_accuracy, test_precision, test_recall, test_f1_score))
psqlconn.commit()

# For merging predictions with data
X_general = df[indicators]
y_logr_pred = logr_model.predict(X_general)

outputs['y_logr_pred'] = y_logr_pred

# XGBoost Model
xgb_data = df.copy(deep=True)
X = xgb_data.drop(['is_fraud'], axis=1)
y = xgb_data['is_fraud']

# Split into train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 4103)

le = OrdinalEncoder(handle_unknown='use_encoded_value',
                                 unknown_value=-1)
for i in X_train.columns:
    if i in cat_features:
        le.fit(np.array(X_train[i]).reshape(-1,1))
        X_train[i]=le.transform(np.array(X_train[i]).reshape(-1,1))
        X_test[i] = le.transform(np.array(X_test[i]).reshape(-1,1))

params = {'subsample': 0.8, 'reg_lambda': 0.8, 'min_child_weight': 1, 'max_depth': 14, 'learning_rate': 0.03, 'gamma': 1.5, 'colsample_bytree': 0.8}

# Instantiate the classifier
xgb_model = XGBClassifier(**params)
xgb_model.fit(X_train, y_train)

# Make predictions on test data
y_pred_test = xgb_model.predict(X_test)

# Evaluation metrics
test_accuracy = accuracy_score(y_test, y_pred_test)
test_precision = precision_score(y_test, y_pred_test)
test_recall = recall_score(y_test, y_pred_test)
test_f1_score = f1_score(y_test, y_pred_test)

cursor.execute(results_query, (1, 'xgb', test_accuracy, test_precision, test_recall, test_f1_score))
psqlconn.commit()

# For merging predictions with data
X_general = df[indicators]
for i in X_general.columns:
    if i in cat_features:
        le.fit(np.array(X_general[i]).reshape(-1,1))
        X_general[i]=le.transform(np.array(X_general[i]).reshape(-1,1))

y_xgb_pred = xgb_model.predict(X_general)

outputs['y_xgb_pred'] = y_xgb_pred

# RandomForest Model
rf_data = df.copy(deep=True)
X = rf_data.drop(['is_fraud'], axis=1)
y = rf_data['is_fraud']

# Split into train, test and validation
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 4103)
X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.25, random_state=4103, stratify=y_train) # 0.25 x 0.8 = 0.2

le = OrdinalEncoder(handle_unknown='use_encoded_value',
                                 unknown_value=-1)
for i in X_train.columns:
    if i in cat_features:
        le.fit(np.array(X_train[i]).reshape(-1,1))
        X_train[i]=le.transform(np.array(X_train[i]).reshape(-1,1))
        X_val[i]= le.transform(np.array(X_val[i]).reshape(-1,1))
        X_test[i] = le.transform(np.array(X_test[i]).reshape(-1,1))

model = RandomForestClassifier(class_weight = "balanced")
model.fit(X_train, y_train)
selection = SelectFromModel(model, threshold=0.035, prefit=True)
select_X_train = selection.transform(X_train)
rf_model = RandomForestClassifier(max_depth = 20, class_weight = "balanced")
rf_model.fit(select_X_train, y_train)

select_X_test = selection.transform(X_test)
predicted_y_test = rf_model.predict(select_X_test)
accuracy_test = accuracy_score(y_test, predicted_y_test)
precision_test = precision_score(y_test, predicted_y_test)
recall_test = recall_score(y_test, predicted_y_test)
f1_test = f1_score(y_test, predicted_y_test)

cursor.execute(results_query, (2, 'rf', accuracy_test, precision_test, recall_test, f1_test))
psqlconn.commit()

# For merging predictions with data
select_X_general = selection.transform(X_general)
y_rf_pred = rf_model.predict(select_X_general)

outputs['y_rf_pred'] = y_rf_pred

# ENSEMBLE
df_ensemble = df.copy(deep=True)

X = df_ensemble.drop(['is_fraud'], axis=1)
y = df_ensemble['is_fraud']

# Split into train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 4103)

le = OrdinalEncoder(handle_unknown='use_encoded_value',
                                 unknown_value=-1)
for i in X_train.columns:
    if i in cat_features:
        le.fit(np.array(X_train[i]).reshape(-1,1))
        X_train[i]=le.transform(np.array(X_train[i]).reshape(-1,1))
        X_test[i] = le.transform(np.array(X_test[i]).reshape(-1,1))

en_logr_model = LogisticRegression(solver = 'liblinear', penalty = 'l1')
en_xgb_model = XGBClassifier(**params)
en_rf_model = RandomForestClassifier(max_depth = 20, class_weight = "balanced")

ensemble_model = VotingClassifier(estimators=[
    ('logr', en_logr_model),
    ('xgb', en_xgb_model),
    ('rf', en_rf_model)
], voting='hard')  # 'hard' for majority voting

ensemble_model.fit(X_train, y_train)

predicted_y_test = ensemble_model.predict(X_test)

accuracy_test = accuracy_score(y_test, predicted_y_test)
precision_test = precision_score(y_test, predicted_y_test)
recall_test = recall_score(y_test, predicted_y_test)
f1_test = f1_score(y_test, predicted_y_test)

cursor.execute(results_query, (3, 'ensemble', accuracy_test, precision_test, recall_test, f1_test))
psqlconn.commit()

# For merging predictions with data
y_ensemble_pred = ensemble_model.predict(X_general)

outputs['y_ensemble_pred'] = y_ensemble_pred

# Merging predicted result back to dataframe
df_output = pd.merge(df_copy, outputs, how = 'left', left_index = True, right_index = True)
df_output.fillna(0, inplace = True)
df_output = df_output.loc[df_output['is_fraud'].ne(0) | df_output['y_logr_pred'].ne(0) | df_output['y_xgb_pred'].ne(0) | df_output['y_rf_pred'].ne(0) | df_output['y_ensemble_pred'].ne(0)]

with engine.connect() as conn:
    print(bool(conn)) # <- just to keep track of the process
    
    df_output.to_sql(name='anomaly_predictions_eth', con=engine, if_exists='replace', index=True)
    print("end") # <- just to keep track of the process


cursor.close()
dbConnection.close()
psqlconn.close()
