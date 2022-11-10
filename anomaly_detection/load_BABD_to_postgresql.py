import psycopg2
import pandas as pd
import json

# Database configurations
with open("/home/ec2-user/etl/extract/config.json") as config_file:
    config = json.load(config_file)

# Connecting to Database
conn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# To execute queries and retrieve data
cursor = conn.cursor()

col_list = ["account", "label"]
df_BABD = pd.read_csv('/home/ec2-user/bitcoin_anomaly/bitcoin_abuse/BABD-13.csv', usecols= col_list)

# Take out only Blackmail, Darknet Market, Gambling, Government Criminal Blacklist, Money Laundering, Ponzi Scheme
df_BABD_post = df_BABD[["account", "label"]]
df_BABD_post = df_BABD_post.loc[df_BABD_post['label'].isin([0, 2, 6, 7, 8, 9])]

# Make label be 1
df_BABD_post["label"] = 1

from sqlalchemy import create_engine
engine = create_engine('postgresql://ec2-user:password@localhost:5432/bitcoin')
df_BABD_post.to_sql('illicit_label', engine)