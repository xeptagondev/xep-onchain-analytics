import pandas as pd
import psycopg2
import json

# Database configurations
with open("config.json") as config_file:
    config = json.load(config_file)

# Connecting to PostgreSQL database
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])



psqlcursor = psqlconn.cursor()

basic_metrics = pd.read_sql("SELECT * FROM basic_metrics", psqlconn)
computed_metrics = pd.read_sql("SELECT * FROM computed_metrics", psqlconn)

bm_eth = pd.read_sql("SELECT * FROM basic_metrics_ethereum", psqlconn)
cm_eth = pd.read_sql("SELECT * FROM computed_metrics_ethereum", psqlconn)

metrics_desc = pd.read_csv("assets/metrics_desc.csv")
md_eth = pd.read_csv("assets/metrics_desc_eth.csv")

bm_dict = {'Bitcoin': basic_metrics, 'Ethereum': bm_eth}
cm_dict = {'Bitcoin': computed_metrics, 'Ethereum': cm_eth}
md_dict = {'Bitcoin': metrics_desc, 'Ethereum': md_eth}

def get_bm_dict():
    return bm_dict

def get_cm_dict():
    return cm_dict

def get_md_dict():
    return md_dict