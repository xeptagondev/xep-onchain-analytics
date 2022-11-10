import pandas as pd
import os

def load_basic_metrics(conn):
    os.chdir('basic_metrics')
    query = """
        CREATE OR REPLACE TABLE basic_metrics AS
        SELECT 
            ATF.Time AS Date,
            ATF."avg(Fee total – Blocks (USD)) – Bitcoin" AS 'Cost Per Transaction',
            ATV."avg(Output total – Blocks (USD)) – Bitcoin" AS 'Average Transaction Value (ATV)',
            CDD."sum(Coindays destroyed – Blocks) – Bitcoin" AS 'Coin-Days-Destroyed (CDD)',
            CS."runningsum(Generated value – Blocks (BTC)) – Bitcoin"/100000000 AS 'Circulating Supply',
            D."max(Difficulty – Blocks) – Bitcoin" AS 'Difficulty',
            P."Price (BTC/USD) – Bitcoin" AS 'Price ($)',
            TC."sum(Transaction count – Blocks) – Bitcoin" AS 'Transaction Count',
            TVBTC."sum(Output total – Blocks (BTC)) – Bitcoin"/100000000 AS 'Transaction Volume (BTC)',
            TVUSD."sum(Output total – Blocks (USD)) – Bitcoin" AS 'Transaction Volume (USD)'
        FROM 
            read_parquet('average_transaction_fee.parquet') ATF,
            read_parquet('average_transaction_value.parquet') ATV,
            read_parquet('cdd.parquet') CDD,
            read_parquet('circulating_supply.parquet') CS,
            read_parquet('difficulty.parquet') D,
            read_parquet('price.parquet') P,
            read_parquet('transaction_count.parquet') TC,
            read_parquet('transaction_volume_btc.parquet') TVBTC,
            read_parquet('transaction_volume_usd.parquet') TVUSD
        WHERE
            ATF.Time == ATV.Time AND
            ATF.Time == CDD.Time AND 
            ATF.Time == CS.Time AND
            ATF.Time == D.Time AND 
            ATF.Time == P.Time AND
            ATF.Time == TC.Time AND
            ATF.Time == TVBTC.Time AND
            ATF.Time == TVUSD.Time
            """
    os.chdir('../')
    conn.execute(query)
    