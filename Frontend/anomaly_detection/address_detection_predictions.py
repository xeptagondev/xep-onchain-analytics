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

# Database configurations
with open('../../config.json') as config_file:
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

select_query = """
SELECT * FROM anomaly_models
"""

models = cursor.execute(select_query)
models = cursor.fetchall()

dtc_model = pickle.loads(codecs.decode(models[0][2].encode(), "base64"))
knn_model = pickle.loads(codecs.decode(models[1][2].encode(), "base64"))
xgb_model = pickle.loads(codecs.decode(models[2][2].encode(), "base64"))

indicators = ['value', 'value_usd', 'is_from_coinbase', 'is_spendable', 'spending_index', 'spending_value_usd'
            , 'lifespan', 'cdd', 'size', 'weight', 'version', 'lock_time', 'is_coinbase', 'has_witness', 'input_count'
            , 'output_count', 'input_total', 'input_total_usd', 'output_total', 'output_total_usd', 'fee', 'fee_usd', 'fee_per_kb'
            , 'fee_per_kb_usd', 'fee_per_kwu', 'fee_per_kwu_usd', 'cdd_total']

# Merging predicted result back to dataframe
X_general = pd.DataFrame(df, columns = indicators)
y_knn_pred = knn_model.predict(X_general)
y_dtc_pred = dtc_model.predict(X_general)
y_xgb_pred = xgb_model.predict(X_general)

outputs = pd.DataFrame()

outputs['y_knn_pred'] = y_knn_pred
outputs['y_dtc_pred'] = y_dtc_pred
outputs['y_xgb_pred'] = y_xgb_pred

df_output = pd.merge(df, outputs, how = 'left', left_index = True, right_index = True)
df_output.fillna(0, inplace = True)
df_output = df_output.loc[df_output['label'].ne(0) | df_output['y_knn_pred'].ne(0) | df_output['y_dtc_pred'].ne(0) | df_output['y_xgb_pred'].ne(0)]

with engine.connect() as conn:
    print(bool(conn)) # <- just to keep track of the process
    
    df_output.to_sql(name='anomaly_predictions', con=engine, if_exists='replace', index=True)
    print("end") # <- just to keep track of the process

cursor.close()
conn.close()
psqlconn.close()