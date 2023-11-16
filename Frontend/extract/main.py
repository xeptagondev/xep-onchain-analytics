from scrape import scrape
from convert import convert
from load_basic_metrics import load_basic_metrics
from download import download
from get_realised_cap import get_realised_cap
from compute import compute, compute_eth
from init_ddb import init_ddb
from load_to_pg import load_to_pg
from load_aws_to_pg import load_aws_data_to_pg
import os
from datetime import date, timedelta
import duckdb as ddb
from sqlalchemy import create_engine
import psycopg2
import json

os.chdir("data")

# Database configurations
with open("../config.json") as config_file:
    config = json.load(config_file)

conn = ddb.connect(config['ddb']['database'])

engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database=config['postgre']['database'],
                            host=config['postgre']['host'],
                            user=config['postgre']['user'],
                            password=config['postgre']['password'],
                            port=config['postgre']['port'])

# Datetime parameters
# for instantiating purposes, can just change the start_date to date.today() - timedelta(days = 31) # 09/10/2023
start_date = date.today() - timedelta(days=14)
# for instantiating purposes, can just change the end_date to date.today() - timedelta(days = 1) # 23/10/2023
end_date = date.today() - timedelta(days=14)

# init_ddb(start_date, end_date, conn) # uncomment this to instantiate the anomaly detection testing database

# local implementation
scrape(start_date, end_date, config)
download(config)
convert(config)
load_basic_metrics(conn, config)
get_realised_cap(start_date, end_date, conn, config)
compute(start_date, end_date, conn)
compute_eth(start_date, end_date, conn)
load_to_pg(engine, conn, start_date, end_date, config)

# uncomment the following to retrieve data for basic & computed metrics from aws s3 bucket and load to pg
load_aws_data_to_pg(config, engine)

conn.close()
psqlconn.close()
