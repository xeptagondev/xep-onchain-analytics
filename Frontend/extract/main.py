from scrape import scrape
from convert import convert
from load_basic_metrics import load_basic_metrics
from download import download
from get_realised_cap import get_realised_cap
from compute import compute, compute_eth
from init_ddb import init_ddb
from load_to_pg import load_to_pg
import os
from datetime import date, timedelta
import duckdb as ddb
from sqlalchemy import create_engine
import psycopg2
import json

os.chdir("data")

# Database configurations
with open("/Users/h.yek/Desktop/4 nov test kl commit/xep-onchain-analytics/Frontend/extract/config.json") as config_file:
    config = json.load(config_file)

conn = ddb.connect(config['ddb']['database'])

engine = create_engine(config['postgre']['engine'])
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

# Datetime parameters
start_date = date.today() - timedelta(days = 73) # for instantiating purposes, can just change the start_date to date.today() - timedelta(days = 31) # 09/10/2023
end_date = date.today() - timedelta(days = 12) # for instantiating purposes, can just change the end_date to date.today() - timedelta(days = 1) # 23/10/2023

# init_ddb(start_date, end_date, conn) # uncomment this to instantiate the anomaly detection testing database

start_date_ = date.today() - timedelta(days = 2912) # 19/12/2016 (2505) but can change to 17/11/2015
end_date_ = date.today() - timedelta(days = 2910)

scrape(start_date_, end_date_, config)
download(config) 
convert(config)
load_basic_metrics(conn, config)
get_realised_cap(start_date, end_date, conn, config)
compute(start_date_, end_date_, conn)
load_to_pg(engine, conn, start_date_, end_date, config)

conn.close()
psqlconn.close()
