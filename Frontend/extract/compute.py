# This file has to code required to calculate and consolidate the computed metrics 

import pandas as pd
from datetime import timedelta, datetime

def compute(start_date, end_date, conn):
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
        filename = "'xep-onchain-analytics/data/inputs/blockchair_bitcoin_inputs_" + day.strftime('%Y%m%d') + ".parquet'"
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
    ## check if table exists

    table_bool = conn.execute("""select count(*) from information_schema.tables where table_name = 'computed_metrics'""")
    
    if table_bool.fetchone()[0] == 1:
        conn.execute(insert_row)
    else:
        conn.execute(create_computed_metrics_table)

    # conn.execute(create_computed_metrics_table)

    # print(conn.execute("select * from computed_metrics").fetchdf())
