import pandas as pd
import psycopg2
import json

# Database configurations
with open("config.json") as config_file:
    config = json.load(config_file)

# Connecting to PostgreSQL database
psqlconn = psycopg2.connect(database=config['postgre']['database'],
                            host=config['postgre']['host'],
                            user=config['postgre']['user'],
                            password=config['postgre']['password'],
                            port=config['postgre']['port'])


psqlcursor = psqlconn.cursor()

cryptocurrencies = ['Bitcoin', 'Ethereum']
crypto_suffix = {'Bitcoin': '', 'Ethereum': '_ethereum'}

bm_dict = {}
cm_dict = {}
md_dict = {}

for crypto in cryptocurrencies:
    basic_metrics_name = 'basic_metrics' + crypto_suffix[crypto]
    computed_metrics_name = 'computed_metrics' + crypto_suffix[crypto]

    basic_metrics_df = pd.read_sql(
        f"SELECT * FROM {basic_metrics_name} ORDER BY \"Date\"", psqlconn)
    computed_metrics_df = pd.read_sql(
        f"SELECT * FROM {computed_metrics_name} ORDER BY \"Date\"", psqlconn)
    metrics_desc_df = pd.read_excel(
        "assets/metrics_desc.xlsx", sheet_name=crypto)

    bm_dict[crypto] = basic_metrics_df
    cm_dict[crypto] = computed_metrics_df
    md_dict[crypto] = metrics_desc_df


def get_bm_dict():
    return bm_dict


def get_cm_dict():
    return cm_dict


def get_md_dict():
    return md_dict
