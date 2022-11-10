from scrape import scrape
from convert import convert
from load_basic_metrics import load_basic_metrics
from download import download
from get_realised_cap import get_realised_cap
from compute import compute
from init_ddb import init_ddb
from load_to_pg import load_to_pg
import os
from datetime import date, timedelta
import duckdb as ddb
from sqlalchemy import create_engine
import psycopg2
import json

os.chdir("xep-onchain-analytics/data")

# Database configurations
with open("extract/config.json") as config_file:
    config = json.load(config_file)

conn = ddb.connect(config['ddb']['database'])

engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# Datetime parameters
start_date = date.today() - timedelta(days = 14) # for instantiating purposes, can just change the start_date to date.today() - timedelta(days = 31)
end_date = date.today() - timedelta(days = 14) # for instantiating purposes, can just change the end_date to date.today() - timedelta(days = 1)

# init_ddb(start_date, end_date, conn) # uncomment this to instantiate the anomaly detection testing database

scrape(start_date, end_date, config)
download() 
convert()
load_basic_metrics(conn)
get_realised_cap(start_date, end_date, conn, config)
compute(start_date, end_date, conn)
load_to_pg(engine, conn, start_date, end_date)

conn.close()
psqlconn.close()
