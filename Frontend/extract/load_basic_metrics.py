import pandas as pd
import os

def get_query(cryptocurrency):
    queries = {
        "bitcoin": """
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
                """, 
        "ethereum": """
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
                AGU."avg(Gas used – Blocks (Gwei)) – Ethereum" AS 'Average Gas Used (Gwei)',
                AGL."avg(Gas limit – Blocks) – Ethereum" AS 'Average Gas Limit',
                NNC."Transactions count – Ethereum" AS 'Number of New Contracts',
                ERCT."ERC-20 transactions count – Ethereum" AS 'ERC-20 Transfers',
            FROM read_parquet('circulation.parquet') CS
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
            LEFT JOIN read_parquet('number-of-new-contracts.parquet') NNC ON CS.Time = NNC.Time
            LEFT JOIN read_parquet('erc-20-transfers.parquet') ERCT on CS.Time = ERCT.Time
                """
    }

    return queries[cryptocurrency]


def load_basic_metrics(conn, config):
    cryptocurrencies = config['cryptocurrencies']

    for crypto in cryptocurrencies:
        basic_metrics_dir = os.path.join(os.getcwd(), crypto, 'basic_metrics')
        # os.chdir(crypto)
        # os.chdir('basic_metrics')
        os.chdir(basic_metrics_dir)
        query = get_query(crypto)
        print(query)
        conn.execute(query)
        # print(conn.execute("SELECT * FROM basic_metrics_ethereum").fetchdf())
        os.chdir('../')
        os.chdir('../')
        print(f"dir aft done with one crypto: {os.getcwd()}")

    # query = """
    #     CREATE OR REPLACE TABLE basic_metrics AS
    #     SELECT 
    #         ATF.Time AS Date,
    #         ATF."avg(Fee total – Blocks (USD)) – Bitcoin" AS 'Cost Per Transaction',
    #         ATV."avg(Output total – Blocks (USD)) – Bitcoin" AS 'Average Transaction Value (ATV)',
    #         CDD."sum(Coindays destroyed – Blocks) – Bitcoin" AS 'Coin-Days-Destroyed (CDD)',
    #         CS."runningsum(Generated value – Blocks (BTC)) – Bitcoin"/100000000 AS 'Circulating Supply',
    #         D."max(Difficulty – Blocks) – Bitcoin" AS 'Difficulty',
    #         P."Price (BTC/USD) – Bitcoin" AS 'Price ($)',
    #         TC."sum(Transaction count – Blocks) – Bitcoin" AS 'Transaction Count',
    #         TVBTC."sum(Output total – Blocks (BTC)) – Bitcoin"/100000000 AS 'Transaction Volume (BTC)',
    #         TVUSD."sum(Output total – Blocks (USD)) – Bitcoin" AS 'Transaction Volume (USD)'
    #     FROM 
    #         read_parquet('average_transaction_fee.parquet') ATF,
    #         read_parquet('average_transaction_value.parquet') ATV,
    #         read_parquet('cdd.parquet') CDD,
    #         read_parquet('circulating_supply.parquet') CS,
    #         read_parquet('difficulty.parquet') D,
    #         read_parquet('price.parquet') P,
    #         read_parquet('transaction_count.parquet') TC,
    #         read_parquet('transaction_volume_btc.parquet') TVBTC,
    #         read_parquet('transaction_volume_usd.parquet') TVUSD
    #     WHERE
    #         ATF.Time == ATV.Time AND
    #         ATF.Time == CDD.Time AND 
    #         ATF.Time == CS.Time AND
    #         ATF.Time == D.Time AND 
    #         ATF.Time == P.Time AND
    #         ATF.Time == TC.Time AND
    #         ATF.Time == TVBTC.Time AND
    #         ATF.Time == TVUSD.Time
    #         """

    