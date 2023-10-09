import pandas as pd
import os

def load_basic_metrics(conn):
    print(f"before os.chdir: {os.getcwd()}")
    os.chdir('/Users/kailing/Documents/GitHub/Capstone/wk_8/xep-onchain-analytics/Frontend/extract/basic_metrics')
    print(f"after os.chdir: {os.getcwd()}")
    query = """
        CREATE OR REPLACE TABLE basic_metrics_ethereum AS
        SELECT 
            CS.Time AS Date,
            CS."runningsum(Generated value – Blocks (ETH)) – Ethereum" AS 'Circulating Supply',
            P."Price (ETH/USD) – Ethereum" AS 'Price ($)',
            TC."sum(Transaction count – Blocks) – Ethereum" AS 'Transaction Count',
            ABS."avg(Block size (kB)) – Ethereum" AS 'Average Block Size',
            AGU."avg(Gas used – Blocks (Gwei)) – Ethereum" AS 'Average Gas Used (Gwei)',
            AGL."avg(Gas limit – Blocks) – Ethereum" AS 'Average Gas Limit',
            ATF."avg(Fee – Transactions (USD)) – Ethereum" AS 'Average Transaction Fee (USD)',
            ATV."avg(Value total – Blocks (USD)) – Ethereum" AS 'Average Transaction Value (USD)'

        FROM 
            read_parquet('circulating_supply.parquet') CS,
            read_parquet('price.parquet') P,
            read_parquet('transaction_count.parquet') TC,
            read_parquet('average_block_size.parquet') ABS,
            read_parquet('average_gas_used.parquet') AGU,
            read_parquet('average_gas_limit.parquet') AGL,
            read_parquet('average_transaction_fee.parquet') ATF,
            read_parquet('average_transaction_value.parquet') ATV,

        WHERE
            CS.Time == P.Time AND
            CS.Time == TC.Time AND
            CS.Time == ABS.Time AND
            CS.Time == AGU.Time AND
            CS.Time == AGL.Time AND
            CS.Time == ATF.Time AND
            CS.Time == ATV.Time
            """
    # os.chdir('../')
    # print(f"after query: {os.getcwd()}")
    conn.execute(query)
    