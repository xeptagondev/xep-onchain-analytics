# This file has to code required to calculate and consolidate the computed metrics 

import pandas as pd
from datetime import timedelta, datetime

def compute(start_date, end_date, conn):
    # Market Capitalisation
    create_market_cap_table = '''
        CREATE TEMP TABLE market_cap AS SELECT "Date", "Price ($)" * "Circulating Supply" AS "Market Capitalisation" 
        FROM basic_metrics_ethereum;
    '''
    conn.execute(create_market_cap_table)
    

    create_computed_metrics_table = '''
        CREATE OR REPLACE TABLE computed_metrics_ethereum AS 
        SELECT m.Date, 
        "Market Capitalisation"
        FROM basic_metrics_ethereum b, market_cap m
        WHERE m.Date = b.Date
    '''

    insert_row = """
        INSERT INTO computed_metrics_ethereum  
        SELECT m.Date, 
        "Market Capitalisation"
        FROM basic_metrics_ethereum b, market_cap m
        WHERE m.Date = b.Date
    """
    ## check if table exists

    table_bool = conn.execute("""select count(*) from information_schema.tables where table_name = 'computed_metrics_ethereum'""")
    
    if table_bool.fetchone()[0] == 1:
        print('inserting row')
        conn.execute(insert_row)
    else:
        print('creating table')
        conn.execute(create_computed_metrics_table)

    # conn.execute(create_computed_metrics_table)

    # print(conn.execute("select * from computed_metrics").fetchdf())

