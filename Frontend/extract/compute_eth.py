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

    # NVT ratio
    create_nvt_table = '''
        CREATE TEMP TABLE nvt_ratio AS SELECT b.Date, "Market Capitalisation" / "Transaction Volume (USD)" AS "Network Value to Transaction (NVT)"
        FROM basic_metrics_ethereum b, market_cap c
        WHERE b.Date = c.Date
    '''
    conn.execute(create_nvt_table)

    # Velocity
    create_velocity_table = '''
        CREATE TEMP TABLE velocity AS SELECT Date, 1 / "Network Value to Transaction (NVT)" AS "Velocity"
        FROM nvt_ratio
    '''
    conn.execute(create_velocity_table)

    # Blocks Mined
    create_blocks_mined_table = '''
        CREATE TEMP TABLE blocks_mined (Date TIMESTAMP, "Blocks Mined" INTEGER)
    '''
    conn.execute(create_blocks_mined_table)

    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        filename = "'/Users/kailing/Documents/GitHub/Capstone/wk_10/xep-onchain-analytics/Frontend/extract/data/blocks/blockchair_ethereum_blocks_" + day.strftime('%Y%m%d') + ".parquet'"
        calculate_blocks_mined = "SELECT max(id) - min(id) + 1 FROM " + filename
        blocks_mined_value = conn.execute(calculate_blocks_mined).fetchone()
        print([day, blocks_mined_value[0]])
        conn.execute('INSERT INTO blocks_mined VALUES (?, ?)', [day, blocks_mined_value[0]])

    # Number of large transactions (at least $100,000)
    create_num_large_t_table = '''
        CREATE TEMP TABLE num_large_t (Date TIMESTAMP,
        "Transaction Count ($0.00 - $1.00)" INTEGER,
        "Transaction Count ($1.00 - $10.00)" INTEGER,
        "Transaction Count ($10.00 - $100.00)" INTEGER,
        "Transaction Count ($100.00 - $1K)" INTEGER,
        "Transaction Count ($1K - $10K)" INTEGER,
        "Transaction Count ($10K - $100K)" INTEGER,
        "Transaction Count ($100K and above)" INTEGER,
        "Transaction Count (Total)" INTEGER)
    '''
    conn.execute(create_num_large_t_table)

    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        filename = "'/Users/kailing/Documents/GitHub/Capstone/wk_10/xep-onchain-analytics/Frontend/extract/data/transactions/blockchair_ethereum_transactions_" + day.strftime('%Y%m%d') + ".parquet'"
        calculate_num_large_t = "SELECT COUNT(CASE WHEN value_usd >= 0 AND value_usd < 1 THEN hash END), COUNT(CASE WHEN value_usd >= 1 AND value_usd < 10 THEN hash END), COUNT(CASE WHEN value_usd >= 10 AND value_usd < 100 THEN hash END), COUNT(CASE WHEN value_usd >= 100 AND value_usd < 1000 THEN hash END), COUNT(CASE WHEN value_usd >= 1000 AND value_usd < 10000 THEN hash END), COUNT(CASE WHEN value_usd >= 10000 AND value_usd < 100000 THEN hash END), COUNT(CASE WHEN value_usd >= 100000 THEN hash END), COUNT(hash) FROM " + filename + " WHERE type != 'synthetic_coinbase'"
        num_large_t_value = conn.execute(calculate_num_large_t).fetchone()
        print([day, num_large_t_value[0], num_large_t_value[1], num_large_t_value[2], num_large_t_value[3], num_large_t_value[4], num_large_t_value[5], num_large_t_value[6], num_large_t_value[7]])
        conn.execute('INSERT INTO num_large_t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', [day, num_large_t_value[0], num_large_t_value[1], num_large_t_value[2], num_large_t_value[3], num_large_t_value[4], num_large_t_value[5], num_large_t_value[6], num_large_t_value[7]])
    
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
        "Network Value to Transaction (NVT)",
        "Velocity",
        "Blocks Mined",
        "Transaction Count ($0.00 - $1.00)",
        "Transaction Count ($1.00 - $10.00)",
        "Transaction Count ($10.00 - $100.00)",
        "Transaction Count ($100.00 - $1K)",
        "Transaction Count ($1K - $10K)",
        "Transaction Count ($10K - $100K)",
        "Transaction Count ($100K and above)",
        "Transaction Count (Total)",
        "Active Addresses",
        "Active Sending Addresses",
        "Active Receiving Addresses"
        FROM basic_metrics_ethereum b
        LEFT JOIN market_cap m ON b.Date = m.Date
        LEFT JOIN nvt_ratio n ON b.Date = n.Date
        LEFT JOIN velocity v ON b.Date = v.Date
        LEFT JOIN blocks_mined bm ON b.Date = bm.Date
        LEFT JOIN num_large_t nlt ON b.Date = nlt.Date
        LEFT JOIN active_addresses a ON b.Date = a.Date
    '''

    insert_row = """
        INSERT INTO computed_metrics_ethereum  
        SELECT b.Date, 
        "Market Capitalisation",
        "Network Value to Transaction (NVT)",
        "Velocity",
        "Blocks Mined",
        "Transaction Count ($0.00 - $1.00)",
        "Transaction Count ($1.00 - $10.00)",
        "Transaction Count ($10.00 - $100.00)",
        "Transaction Count ($100.00 - $1K)",
        "Transaction Count ($1K - $10K)",
        "Transaction Count ($10K - $100K)",
        "Transaction Count ($100K and above)",
        "Transaction Count (Total)",
        "Active Addresses",
        "Active Sending Addresses",
        "Active Receiving Addresses",
        FROM basic_metrics_ethereum b
        LEFT JOIN market_cap m ON b.Date = m.Date
        LEFT JOIN nvt_ratio n ON b.Date = n.Date
        LEFT JOIN velocity v ON b.Date = v.Date
        LEFT JOIN blocks_mined bm ON b.Date = bm.Date
        LEFT JOIN num_large_t nlt ON b.Date = nlt.Date
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