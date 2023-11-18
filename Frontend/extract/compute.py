# This file has to code required to calculate and consolidate the computed metrics

import pandas as pd
from datetime import timedelta, datetime
from datetime import date
import pyarrow.parquet as pq
import os


def compute(start_date, end_date, conn):
    '''Creates/replaces computed_metrics table if table has 0 rows or inserts values into computed_metrics table in duckdb for Bitcoin.'''
    os.chdir('bitcoin')

    # Market Capitalisation
    create_market_cap_table = '''
        CREATE TEMP TABLE market_cap AS SELECT "Date", "Price ($)" * "Circulating Supply" AS "Market Capitalisation" 
        FROM basic_metrics;
    '''
    conn.execute(create_market_cap_table)

    # NVT ratio
    create_nvt_table = '''
        CREATE TEMP TABLE nvt_ratio AS SELECT b.Date, "Market Capitalisation" / "Transaction Volume (USD)" AS "Network Value to Transaction (NVT)"
        FROM basic_metrics b, market_cap c
        WHERE b.Date = c.Date
    '''
    conn.execute(create_nvt_table)

    # SOPR
    create_sopr_table = '''
        CREATE TEMP TABLE sopr (Date TIMESTAMP, "Spent Output Profit Ratio (SOPR)" DOUBLE)
    '''

    conn.execute(create_sopr_table)

    delta = end_date - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        filename = "'xep-onchain-analytics/data/inputs/blockchair_bitcoin_inputs_" + \
            day.strftime('%Y%m%d') + ".parquet'"
        calculate_sopr = "SELECT sum(spending_value_usd) / sum(value_usd) FROM " + filename
        sopr_value = conn.execute(calculate_sopr).fetchone()
        conn.execute('INSERT INTO sopr VALUES (?, ?)', [day, sopr_value[0]])

    # Net Realised Profit/Loss
    create_net_realised_profit = '''
        CREATE TEMP TABLE net_realised_profit AS SELECT b.Date, (1 - (1 / "Spent Output Profit Ratio (SOPR)")) * "Transaction Volume (USD)" AS "Net Realised Profit/Loss"
        FROM sopr s, basic_metrics b
        WHERE s.Date = b.Date
    '''
    conn.execute(create_net_realised_profit)

    # Average Coin Dormancy
    create_acd_table = '''
        CREATE TEMP TABLE acd AS SELECT Date, "Coin-Days-Destroyed (CDD)" / "Transaction Volume (BTC)" AS "Average Coin Dormancy"
        FROM basic_metrics
    '''
    conn.execute(create_acd_table)

    # Velocity
    create_velocity_table = '''
        CREATE TEMP TABLE velocity AS SELECT Date, 1 / "Network Value to Transaction (NVT)" AS "Velocity"
        FROM nvt_ratio
    '''
    conn.execute(create_velocity_table)

    # Market Cap to Thermocap Ratio
    create_thermocap = '''
        CREATE TEMP TABLE thermocap AS 
        SELECT Date, SUM("Cost Per Transaction" * "Transaction Count") OVER (ORDER BY Date) AS "Thermocap"
        FROM basic_metrics
        GROUP BY Date, "Cost Per Transaction", "Transaction Count"
    '''
    conn.execute(create_thermocap)

    # Balanced Price
    create_rolling_cdd = '''
        CREATE TEMP TABLE rolling_cdd AS 
        SELECT Date, SUM("Coin-Days-Destroyed (CDD)" * "Price ($)") OVER (ORDER BY Date) AS "Cumulative CDD (USD)",
        DATEDIFF('day', DATE '2009-1-3', Date)+1 AS days
        FROM basic_metrics
        GROUP BY Date, "Coin-Days-Destroyed (CDD)", "Price ($)"
    '''
    conn.execute(create_rolling_cdd)

    create_realised_price = '''
        CREATE TEMP TABLE realised_price AS
        SELECT b.Date, "Realised Cap USD" / "Circulating Supply" AS "Realised Price"
        FROM basic_metrics b, realised_cap r
        WHERE b.Date = r.Date
    '''
    conn.execute(create_realised_price)

    create_computed_metrics_table = '''
        CREATE OR REPLACE TABLE computed_metrics AS 
        SELECT r.Date, 
        "Market Capitalisation", 
        "Realised Cap USD" AS "Realised Capitalisation",
        "Realised Price",
        "Network Value to Transaction (NVT)",
        "Spent Output Profit Ratio (SOPR)",
        "Thermocap",
        "Market Capitalisation" / "Realised Cap USD" AS "Market Value to Realised Value Ratio (MVRV)", 
        ("Market Capitalisation" - "Realised Cap USD") / "Market Capitalisation" AS "Net Unrealised Profit and Loss (NUPL)",
        ("Price ($)" * "Realised Cap BTC") - ("Realised Cap USD") AS "Relative Unrealised Profit",
        1 / "Network Value to Transaction (NVT)" AS "Velocity",
        (1 - (1 / "Spent Output Profit Ratio (SOPR)")) * "Transaction Volume (USD)" AS "Net Realised Profit/Loss",
        "Coin-Days-Destroyed (CDD)" / "Transaction Volume (BTC)" AS "Average Coin Dormancy",
        "Market Capitalisation" / "Thermocap" AS "Market Cap to Thermocap Ratio",
        ("Cumulative CDD (USD)" / (days * "Circulating Supply")) AS "Transferred Price",
        "Realised Price" - ("Cumulative CDD (USD)" / (days * "Circulating Supply")) AS "Balanced Price",
        "Realised Cap USD" - "Thermocap" AS "Investor Capitalisation"
        FROM basic_metrics b, market_cap m, realised_cap r, nvt_ratio n, sopr s, thermocap t, rolling_cdd c, realised_price p
        WHERE r.Date = b.Date
        AND m.Date = b.Date
        AND n.Date = b.Date
        AND s.Date = b.Date
        AND t.Date = b.Date
        AND c.Date = b.Date
        AND p.Date = b.Date
    '''

    insert_row = """
        INSERT INTO computed_metrics  
        SELECT r.Date, 
        "Market Capitalisation", 
        "Realised Cap USD" AS "Realised Capitalisation",
        "Realised Price",
        "Network Value to Transaction (NVT)",
        "Spent Output Profit Ratio (SOPR)",
        "Thermocap",
        "Market Capitalisation" / "Realised Cap USD" AS "Market Value to Realised Value Ratio (MVRV)", 
        ("Market Capitalisation" - "Realised Cap USD") / "Market Capitalisation" AS "Net Unrealised Profit and Loss (NUPL)",
        ("Price ($)" * "Realised Cap BTC") - ("Realised Cap USD") AS "Relative Unrealised Profit",
        1 / "Network Value to Transaction (NVT)" AS "Velocity",
        (1 - (1 / "Spent Output Profit Ratio (SOPR)")) * "Transaction Volume (USD)" AS "Net Realised Profit/Loss",
        "Coin-Days-Destroyed (CDD)" / "Transaction Volume (BTC)" AS "Average Coin Dormancy",
        "Market Capitalisation" / "Thermocap" AS "Market Cap to Thermocap Ratio",
        ("Cumulative CDD (USD)" / (days * "Circulating Supply")) AS "Transferred Price",
        "Realised Price" - ("Cumulative CDD (USD)" / (days * "Circulating Supply")) AS "Balanced Price",
        "Realised Cap USD" - "Thermocap" AS "Investor Capitalisation"
        FROM basic_metrics b, market_cap m, realised_cap r, nvt_ratio n, sopr s, thermocap t, rolling_cdd c, realised_price p
        WHERE r.Date = b.Date
        AND m.Date = b.Date
        AND n.Date = b.Date
        AND s.Date = b.Date
        AND t.Date = b.Date
        AND c.Date = b.Date
        AND p.Date = b.Date
    """
    # check if table exists

    table_bool = conn.execute(
        """select count(*) from information_schema.tables where table_name = 'computed_metrics'""")

    if table_bool.fetchone()[0] == 1:
        conn.execute(insert_row)
    else:
        conn.execute(create_computed_metrics_table)

    # conn.execute(create_computed_metrics_table)

    # print(conn.execute("select * from computed_metrics").fetchdf())


def compute_eth(start_date, end_date, conn):
    '''Creates or replaces computed_metrics_ethereum table in duckdb for Ethereum.'''
    print(f"curr working dir: {os.getcwd()}")
    os.chdir('ethereum')
    print(f"checking if inside eth folder: {os.getcwd()}")
    delta = end_date - start_date

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

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        filename = "'blocks/blockchair_ethereum_blocks_" + \
            day.strftime('%Y%m%d') + ".parquet'"
        calculate_blocks_mined = "SELECT max(id) - min(id) + 1 FROM " + \
            filename
        blocks_mined_value = conn.execute(calculate_blocks_mined).fetchone()
        print([day, blocks_mined_value[0]])
        conn.execute('INSERT INTO blocks_mined VALUES (?, ?)',
                     [day, blocks_mined_value[0]])

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

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        filename = "'transactions/blockchair_ethereum_transactions_" + \
            day.strftime('%Y%m%d') + ".parquet'"
        calculate_num_large_t = "SELECT COUNT(CASE WHEN value_usd >= 0 AND value_usd < 1 THEN hash END), COUNT(CASE WHEN value_usd >= 1 AND value_usd < 10 THEN hash END), COUNT(CASE WHEN value_usd >= 10 AND value_usd < 100 THEN hash END), COUNT(CASE WHEN value_usd >= 100 AND value_usd < 1000 THEN hash END), COUNT(CASE WHEN value_usd >= 1000 AND value_usd < 10000 THEN hash END), COUNT(CASE WHEN value_usd >= 10000 AND value_usd < 100000 THEN hash END), COUNT(CASE WHEN value_usd >= 100000 THEN hash END), COUNT(hash) FROM " + filename + " WHERE type != 'synthetic_coinbase'"
        num_large_t_value = conn.execute(calculate_num_large_t).fetchone()
        print([day, num_large_t_value[0], num_large_t_value[1], num_large_t_value[2], num_large_t_value[3],
              num_large_t_value[4], num_large_t_value[5], num_large_t_value[6], num_large_t_value[7]])
        conn.execute('INSERT INTO num_large_t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', [
                     day, num_large_t_value[0], num_large_t_value[1], num_large_t_value[2], num_large_t_value[3], num_large_t_value[4], num_large_t_value[5], num_large_t_value[6], num_large_t_value[7]])

    # Active Addresses, Active Sending Addresses, Active Receiving Addresses
    create_active_addresses_table = '''
        CREATE TEMP TABLE active_addresses (Date TIMESTAMP, "Active Addresses" INTEGER, "Active Sending Addresses" INTEGER, "Active Receiving Addresses" INTEGER)
    '''
    conn.execute(create_active_addresses_table)

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        day = datetime.combine(day, datetime.min.time())
        print(f"getting address data for {day}")
        filename = "transactions/blockchair_ethereum_transactions_" + \
            day.strftime('%Y%m%d') + ".parquet"
        print(filename)
        transactions_df = pq.read_table(filename).to_pandas()
        # print(transactions_df)
        transactions_df = transactions_df[~transactions_df['type'].isin(
            ['synthetic_coinbase'])][['index', 'type', 'sender', 'recipient']]
        all_unique_transactions_df = pd.concat(
            [transactions_df['sender'], transactions_df['recipient']]).unique()
        total_active_addresses = len(all_unique_transactions_df)

        # Active Sending Addresses
        unique_sending_transactions_df = transactions_df['sender'].unique()
        total_active_sending_addresses = len(unique_sending_transactions_df)

        # Active Receiving Addresses
        unique_receiving_transactions_df = transactions_df['recipient'].unique(
        )
        total_active_receiving_addresses = len(
            unique_receiving_transactions_df)

        print("day, total_active_addresses, total_active_sending_addresses, total_active_receiving_addresses:")
        print(day)
        print(total_active_addresses)
        print(total_active_sending_addresses)
        print(total_active_receiving_addresses)
        conn.execute('INSERT INTO active_addresses VALUES (?, ?, ?, ?)', [
                     day, total_active_addresses, total_active_sending_addresses, total_active_receiving_addresses])

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

    # insert_row = """
    #     INSERT INTO computed_metrics_ethereum
    #     SELECT b.Date,
    #     "Market Capitalisation",
    #     "Network Value to Transaction (NVT)",
    #     "Velocity",
    #     "Blocks Mined",
    #     "Transaction Count ($0.00 - $1.00)",
    #     "Transaction Count ($1.00 - $10.00)",
    #     "Transaction Count ($10.00 - $100.00)",
    #     "Transaction Count ($100.00 - $1K)",
    #     "Transaction Count ($1K - $10K)",
    #     "Transaction Count ($10K - $100K)",
    #     "Transaction Count ($100K and above)",
    #     "Transaction Count (Total)",
    #     "Active Addresses",
    #     "Active Sending Addresses",
    #     "Active Receiving Addresses",
    #     FROM basic_metrics_ethereum b
    #     LEFT JOIN market_cap m ON b.Date = m.Date
    #     LEFT JOIN nvt_ratio n ON b.Date = n.Date
    #     LEFT JOIN velocity v ON b.Date = v.Date
    #     LEFT JOIN blocks_mined bm ON b.Date = bm.Date
    #     LEFT JOIN num_large_t nlt ON b.Date = nlt.Date
    #     LEFT JOIN active_addresses a ON b.Date = a.Date
    # """

    # table_bool = conn.execute("""select count(*) from information_schema.tables where table_name = 'computed_metrics_ethereum'""")

    # if table_bool.fetchone()[0] == 1:
    #     print('inserting row')
    #     conn.execute(insert_row)
    # else:
    #     print('creating table')
    #     conn.execute(create_computed_metrics_table)

    conn.execute(create_computed_metrics_table)
