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

# Database configurations
with open('../config.json') as config_file:
    config = json.load(config_file)

# Retrieve dataset from duck db
conn = ddb.connect(config['ddb']['database'])
df = conn.execute("SELECT * from anomaly_df").fetchdf()

# Connecting to Database
engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database = config['postgre_extract']['database'],
                            host = config['postgre_extract']['host'],
                            user = config['postgre_extract']['user'],
                            password = config['postgre_extract']['password'],
                            port = config['postgre_extract']['port'])

# To execute queries and retrieve data
cursor = psqlconn.cursor()
dbConnection = engine.connect()

# reduce number of rows used in training
df_0 = df.loc[df['label'] == 0]
df_1 = df.loc[df['label'] == 1]
print(len(df_0.index))
print(len(df_1.index))

df_copy = df.copy()

y = df_copy.pop('label')
X = df_copy
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state= 4103)

indicators = ['value', 'value_usd', 'is_from_coinbase', 'is_spendable', 'spending_index', 'spending_value_usd'
            , 'lifespan', 'cdd', 'size', 'weight', 'version', 'lock_time', 'is_coinbase', 'has_witness', 'input_count'
            , 'output_count', 'input_total', 'input_total_usd', 'output_total', 'output_total_usd', 'fee', 'fee_usd', 'fee_per_kb'
            , 'fee_per_kb_usd', 'fee_per_kwu', 'fee_per_kwu_usd', 'cdd_total']

X_train = pd.DataFrame(X_train, columns = indicators)
X_test = pd.DataFrame(X_test, columns = indicators)

# DECISION TREE CLASSIFIER
dtc_model = DecisionTreeClassifier(random_state = 0, criterion='entropy', splitter='best')

dtc_model.fit(X_train, y_train)
y_train_dtc_pred = dtc_model.predict(X_train)
# Training Accuracy
print("Train Accuracy DTC:", accuracy_score(y_train, y_train_dtc_pred))

y_test_dtc_pred = dtc_model.predict(X_test)
# Testing Accuracy
print("Test Accuracy DTC:", accuracy_score(y_test, y_test_dtc_pred))

# KNN NEIGHBOURS
knn_model = KNeighborsClassifier(n_neighbors=4, algorithm='kd_tree', weights='distance')
knn_model.fit(X_train, y_train)

y_train_knn_pred = knn_model.predict(X_train)
# Training Accuracy
print("Train Accuracy KNN:", accuracy_score(y_train, y_train_knn_pred))

y_test_knn_pred = knn_model.predict(X_test)
# Testing Accuracy
print("Test Accuracy KNN:", accuracy_score(y_test, y_test_knn_pred))

# XGB
params = {"objective":"multi:softmax", "num_class": 13,'learning_rate': 0.5, "eval_metric": "mlogloss", 'n_estimators':100}
xgb_model = XGBClassifier(**params)

# Training XGBoost model
xgb_model.fit(X_train, y_train)

# running on Train and Test
y_train_xgb_pred = xgb_model.predict(X_train)
print("Train Accuracy XGB:", accuracy_score(y_train, y_train_xgb_pred))

y_test_xgb_pred = xgb_model.predict(X_test)
print("Test Accuracy XGB:", accuracy_score(y_test, y_test_xgb_pred))

# Pickle models
pickle_classifier_string_dtc = pickle.dumps(dtc_model)
pickle_classifier_string_knn = pickle.dumps(knn_model)
pickle_classifier_string_xgb = pickle.dumps(xgb_model)

classifier = ["dtc", "knn", "xgb"]
models = [pickle_classifier_string_dtc, pickle_classifier_string_knn, pickle_classifier_string_xgb]
y_pred_train_model = [y_train_dtc_pred, y_train_knn_pred, y_train_xgb_pred]
y_pred_test_model = [y_test_dtc_pred, y_test_knn_pred, y_test_xgb_pred]

print(bool(dbConnection)) # <- just to keep track of the process
df = pd.DataFrame(columns=['class', 'model'])
df.to_sql(name='anomaly_models', con=engine, if_exists='replace', index=True, index_label= "id")

for i in range(0,3):
    print(i)
    pickled = codecs.encode(models[i], "base64").decode()

    query = "INSERT INTO anomaly_models VALUES (%s, %s, %s)"
    cursor.execute(query, (i, classifier[i], pickled))
    psqlconn.commit()

    print("commit model")


df = pd.DataFrame(columns=['class', 'train_acc', 'train_precision', 'train_recall', 'train_f1score', 'test_acc', 'test_precision', 'test_recall', 'test_f1score'])
df.to_sql(name='anomaly_results', con=engine, if_exists='replace', index=True, index_label= "id")

for i in range(0,3):
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

    query = "INSERT INTO anomaly_results VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (i, classifier[i], train_accuracy, train_precision, train_recall, train_f1_score, test_accuracy, test_precision, test_recall, test_f1_score))
    psqlconn.commit()

cursor.close()
print("end") # <- just to keep track of the process


dbConnection.close()
psqlconn.close()