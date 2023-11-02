# This file has to code required to calculate and consolidate the computed metrics 

import pandas as pd
from datetime import timedelta, datetime
import pyarrow.parquet as pq

def compute(start_date, end_date, conn):
    # Market Capitalisation
    create_market_cap_table = '''
        CREATE TEMP TABLE market_cap AS SELECT "Date", "Price ($)" * "Circulating Supply" AS "Market Capitalisation" 
        FROM basic_metrics_ethereum;
    '''
    conn.execute(create_market_cap_table)
    
    # Active Addresses, Active Sending Addresses, Active Receiving Addresses
    create_active_addresses_table = '''
        CREATE TEMP TABLE active_addresses (Date TIMESTAMP, "Active Addresses" INTEGER, "Active Sending Addresses" INTEGER, "Active Receiving Addresses" INTEGER)
    '''
    conn.execute(create_active_addresses_table)

    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        print(f"getting address data for {day}")
        filename = "/Users/h.yek/Desktop/eth-kl with kl extract folder/xep-onchain-analytics/Frontend/extract/Downloads/Ethereum/addresses/blockchair_ethereum_transactions_" + day.strftime('%Y%m%d') + ".parquet"
        print(filename)
        transactions_df = pq.read_table(filename).to_pandas()
        # print(transactions_df)
        transactions_df = transactions_df[~transactions_df['type'].isin(['synthetic_coinbase'])][['index', 'type', 'sender', 'recipient']]
        all_unique_transactions_df = pd.concat([transactions_df['sender'],transactions_df['recipient']]).unique()
        total_active_addresses = len(all_unique_transactions_df)

        # Active Sending Addresses
        unique_sending_transactions_df = transactions_df['sender'].unique()
        total_active_sending_addresses = len(unique_sending_transactions_df)
        
        #  Active Receiving Addresses
        unique_receiving_transactions_df = transactions_df['recipient'].unique()
        total_active_receiving_addresses = len(unique_receiving_transactions_df)

        print("day, total_active_addresses, total_active_sending_addresses, total_active_receiving_addresses:")
        print(day)
        print(total_active_addresses)
        print(total_active_sending_addresses)
        print(total_active_receiving_addresses)
        # print([day, total_active_addresses, total_active_sending_addresses, total_active_receiving_addresses]+'\n')
        conn.execute('INSERT INTO active_addresses VALUES (?, ?, ?, ?)', [day, total_active_addresses, total_active_sending_addresses, total_active_receiving_addresses])

    create_computed_metrics_table = '''
        CREATE OR REPLACE TABLE computed_metrics_ethereum AS 
        SELECT b.Date, 
        "Market Capitalisation",
        "Active Addresses",
        "Active Sending Addresses",
        "Active Receiving Addresses"
        FROM basic_metrics_ethereum b
        LEFT JOIN market_cap m ON b.Date = m.Date
        LEFT JOIN active_addresses a ON b.Date = a.Date
    '''

    insert_row = """
        INSERT INTO computed_metrics_ethereum  
        SELECT b.Date, 
        "Market Capitalisation",
        "Active Addresses",
        "Active Sending Addresses",
        "Active Receiving Addresses",
        FROM basic_metrics_ethereum b
        LEFT JOIN market_cap m ON b.Date = m.Date
        LEFT JOIN active_addresses a ON b.Date = a.Date
    """
    ## check if table exists

    # table_bool = conn.execute("""select count(*) from information_schema.tables where table_name = 'computed_metrics_ethereum'""")
    
    # if table_bool.fetchone()[0] == 1:
    #     print('inserting row')
    #     conn.execute(insert_row)
    # else:
    #     print('creating table')
    #     conn.execute(create_computed_metrics_table)

    # overwrite table everytime the script runs else will keep adding more & more rows
    conn.execute(create_computed_metrics_table)

    # print(conn.execute("select * from computed_metrics").fetchdf())