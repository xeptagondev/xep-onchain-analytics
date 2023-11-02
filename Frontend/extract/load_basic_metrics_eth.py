import pandas as pd
import os
from datetime import date

def load_basic_metrics(conn):
    print(f"before os.chdir: {os.getcwd()}")
    download_main_dir = os.path.join(os.getcwd(), 'data', 'basic_metrics', 'Ethereum')
    dir = os.path.join(download_main_dir,str(date.today()))
    os.chdir(dir)
    # os.chdir('/Users/kailing/Documents/GitHub/Capstone/wk_8/xep-onchain-analytics/Frontend/extract/basic_metrics')
    print(f"after os.chdir: {os.getcwd()}")
    query = """
        CREATE OR REPLACE TABLE basic_metrics_ethereum AS
        SELECT 
            CS.Time AS Date,
            CS."runningsum(Generated value – Blocks (ETH)) – Ethereum" AS 'Circulating Supply',
            P."Price (ETH/USD) – Ethereum" AS 'Price ($)',
            TC."sum(Transaction count – Blocks) – Ethereum" AS 'Transaction Count',
            ATF."avg(Fee – Transactions (USD)) – Ethereum" AS 'Average Transaction Fee (USD)',
            ATV."avg(Value total – Blocks (USD)) – Ethereum" AS 'Average Transaction Value (USD)',
            TV."sum(Value total – Blocks (USD)) – Ethereum" AS 'Transaction Volume (USD)',
            ABS."avg(Block size (kB)) – Ethereum" AS 'Average Block Size',
            ABI."f(number(86400)/count()) – Ethereum" AS 'Average Block Interval (s)',
            AGP."avg(Gas price) – Ethereum" AS 'Average Gas Price (Gwei)',
            AGU."avg(Gas used – Blocks (Gwei)) – Ethereum" AS 'Average  (Gwei)',
            AGL."avg(Gas limit – Blocks) – Ethereum" AS 'Average Gas Limit',
            D."max(Difficulty – Blocks) – Ethereum" AS 'Difficulty',
            NNC."Transactions count – Ethereum" AS 'Number of New Contracts',
            DSB."BurntFees" AS 'Daily Supply Burned' 
        FROM 
            read_parquet('circulation.parquet') CS
        LEFT JOIN read_parquet('price.parquet') P ON CS.Time = P.Time
        LEFT JOIN read_parquet('transaction-count.parquet') TC ON CS.Time = TC.Time
        LEFT JOIN read_parquet('average-transaction-fee-usd.parquet') ATF ON CS.Time = ATF.Time
        LEFT JOIN read_parquet('average-transaction-amount-usd.parquet') ATV ON CS.Time = ATV.Time
        LEFT JOIN read_parquet('transaction-volume-usd.parquet') TV ON CS.Time = TV.Time
        LEFT JOIN read_parquet('average-block-size.parquet') ABS ON CS.Time = ABS.Time
        LEFT JOIN read_parquet('average-block-interval.parquet') ABI ON CS.Time = ABI.Time
        LEFT JOIN read_parquet('average-gas-price.parquet') AGP ON CS.Time = AGP.Time
        LEFT JOIN read_parquet('average-gas-used.parquet') AGU ON CS.Time = AGU.Time
        LEFT JOIN read_parquet('average-gas-limit.parquet') AGL ON CS.Time = AGL.Time
        LEFT JOIN read_parquet('difficulty.parquet') D ON CS.Time = D.Time
        LEFT JOIN read_parquet('number-of-new-contracts.parquet') NNC ON CS.Time = NNC.Time
        LEFT JOIN read_parquet('dailyethburnt.parquet') DSB ON CS.Time = DSB.Time
            """
    # os.chdir('../')
    # print(f"after query: {os.getcwd()}")
    conn.execute(query)
    