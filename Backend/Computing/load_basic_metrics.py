#Creating Tables For Basic Metrics
def load_basic_metrics(cwd, conn):
    if cwd.startswith('s3://'):
        query = """ 
            CREATE OR REPLACE TABLE basic_metrics AS
            SELECT 
                DATE_TRUNC('day', CAST(ATF.Time AS TIMESTAMP)) AS Date,
                ATF."avg(Fee – Transactions (USD)) – Bitcoin" AS 'Cost Per Transaction',
                ATV."avg(Output total – Blocks (USD)) – Bitcoin" AS 'Average Transaction Value (ATV)',
                CDD."sum(Coindays destroyed – Blocks) – Bitcoin" AS 'Coin-Days-Destroyed (CDD)',
                CS."runningsum(Generated value – Blocks (BTC)) – Bitcoin"/100000000 AS 'Circulating Supply',
                D."max(Difficulty – Blocks) – Bitcoin" AS 'Difficulty',
                P."Price (BTC/USD) – Bitcoin" AS 'Price ($)',
                TC."sum(Transaction count – Blocks) – Bitcoin" AS 'Transaction Count',
                TVBTC."sum(Output total – Blocks (BTC)) – Bitcoin"/100000000 AS 'Transaction Volume (BTC)',
                TVUSD."sum(Output total – Blocks (USD)) – Bitcoin" AS 'Transaction Volume (USD)'
            FROM 
                "average-transaction-fee-usd" ATF,
                "average-transaction-amount-usd" ATV,
                "coindays-destroyed" CDD,
                "circulation" CS,
                "difficulty" D,
                "price" P,
                "transaction-count" TC,
                "transaction-volume" TVBTC,
                "transaction-volume-usd" TVUSD
            WHERE
                ATF.Time = ATV.Time AND
                ATF.Time = CDD.Time AND 
                ATF.Time = CS.Time AND
                ATF.Time = D.Time AND 
                ATF.Time = P.Time AND
                ATF.Time = TC.Time AND
                ATF.Time = TVBTC.Time AND
                ATF.Time = TVUSD.Time
                """
        
    else:    
        query = """
            CREATE OR REPLACE TABLE basic_metrics AS
            SELECT 
                ATF.Time AS Date,
                ATF."avg(Fee – Transactions (USD)) – Bitcoin" AS 'Cost Per Transaction',
                ATV."avg(Output total – Blocks (USD)) – Bitcoin" AS 'Average Transaction Value (ATV)',
                CDD."sum(Coindays destroyed – Blocks) – Bitcoin" AS 'Coin-Days-Destroyed (CDD)',
                CS."runningsum(Generated value – Blocks (BTC)) – Bitcoin"/100000000 AS 'Circulating Supply',
                D."max(Difficulty – Blocks) – Bitcoin" AS 'Difficulty',
                P."Price (BTC/USD) – Bitcoin" AS 'Price ($)',
                TC."sum(Transaction count – Blocks) – Bitcoin" AS 'Transaction Count',
                TVBTC."sum(Output total – Blocks (BTC)) – Bitcoin"/100000000 AS 'Transaction Volume (BTC)',
                TVUSD."sum(Output total – Blocks (USD)) – Bitcoin" AS 'Transaction Volume (USD)'
            FROM 
                read_parquet('average-transaction-fee-usd.parquet') ATF,
                read_parquet('average-transaction-amount-usd.parquet') ATV,
                read_parquet('coindays-destroyed.parquet') CDD,
                read_parquet('circulation.parquet') CS,
                read_parquet('difficulty.parquet') D,
                read_parquet('price.parquet') P,
                read_parquet('transaction-count.parquet') TC,
                read_parquet('transaction-volume.parquet') TVBTC,
                read_parquet('transaction-volume-usd.parquet') TVUSD
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
    conn.execute(query)


    