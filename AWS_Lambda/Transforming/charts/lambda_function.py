import json
import boto3
import pyarrow.parquet as pq
import io
import duckdb as ddb
import pandas as pd
import pyarrow as pa

# Read Config.json
with open("config.json") as json_file:
    config = json.load(json_file)

# Connect to S3 bucket
print("Connecting to S3 bucket")
session = boto3.Session(aws_access_key_id = config['ACCESS_KEY'], aws_secret_access_key = config['SECRET_KEY'])
s3_client = session.client('s3')
print("Connected to S3 bucket")



def handler(event = None, context= None):

    conn = ddb.connect()
    bucket = "onchain-downloads"
    # read all the dataframes

    join_table_query = """
            CREATE OR REPLACE TABLE basic_metrics_ethereum AS
            SELECT 
                CS.Time AS Date,
                CS."runningsum(Generated value – Blocks (ETH)) – Ethereum" AS 'Circulating Supply',
                P."Price (ETH/USD) – Ethereum" AS 'Price ($)',
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
            FROM circulation_df CS
            LEFT JOIN price_df P ON CS.Time = P.Time
            LEFT JOIN average_transaction_fee_usd_df ATF ON CS.Time = ATF.Time
            LEFT JOIN average_transaction_amount_usd_df ATV ON CS.Time = ATV.Time
            LEFT JOIN transaction_volume_usd_df TV ON CS.Time = TV.Time
            LEFT JOIN average_block_size_df ABS ON CS.Time = ABS.Time
            LEFT JOIN average_block_interval_df ABI ON CS.Time = ABI.Time
            LEFT JOIN average_gas_price_df AGP ON CS.Time = AGP.Time
            LEFT JOIN average_gas_used_df AGU ON CS.Time = AGU.Time
            LEFT JOIN average_gas_limit_df AGL ON CS.Time = AGL.Time
            LEFT JOIN number_of_new_contracts_df NNC ON CS.Time = NNC.Time
            LEFT JOIN erc_20_transfers_df ERCT on CS.Time = ERCT.Time
                """

    price_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/price.parquet")
    price_df = pq.read_table(io.BytesIO(price_df['Body'].read()))
    average_transaction_fee_usd_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-transaction-fee-usd.parquet")
    average_transaction_fee_usd_df = pq.read_table(io.BytesIO(average_transaction_fee_usd_df['Body'].read()))
    average_transaction_amount_usd_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-transaction-amount-usd.parquet")
    average_transaction_amount_usd_df = pq.read_table(io.BytesIO(average_transaction_amount_usd_df['Body'].read()))
    transaction_volume_usd_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/transaction-volume-usd.parquet")
    transaction_volume_usd_df = pq.read_table(io.BytesIO(transaction_volume_usd_df['Body'].read()))
    average_block_size_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-block-size.parquet")
    average_block_size_df = pq.read_table(io.BytesIO(average_block_size_df['Body'].read()))
    average_block_interval_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-block-interval.parquet")
    average_block_interval_df = pq.read_table(io.BytesIO(average_block_interval_df['Body'].read()))
    average_gas_price_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-gas-price.parquet")
    average_gas_price_df = pq.read_table(io.BytesIO(average_gas_price_df['Body'].read()))
    average_gas_used_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-gas-used.parquet")
    average_gas_used_df = pq.read_table(io.BytesIO(average_gas_used_df['Body'].read()))
    average_gas_limit_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/average-gas-limit.parquet")
    average_gas_limit_df = pq.read_table(io.BytesIO(average_gas_limit_df['Body'].read()))
    number_of_new_contracts_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/number-of-new-contracts.parquet")
    number_of_new_contracts_df = pq.read_table(io.BytesIO(number_of_new_contracts_df['Body'].read()))
    erc_20_transfers_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/erc-20-transfers.parquet")
    erc_20_transfers_df = pq.read_table(io.BytesIO(erc_20_transfers_df['Body'].read()))
    circulation_df = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/charts/circulation.parquet")
    circulation_df = pq.read_table(io.BytesIO(circulation_df['Body'].read()))


    # Combine the dataframes
    conn.execute(join_table_query)

    # Compute

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

    # Join the tables into one
    join_result_table_query = '''
        CREATE OR REPLACE TABLE computed_metrics_ethereum AS 
        SELECT b.Date,
        "Market Capitalisation",
        "Network Value to Transaction (NVT)",
        "Velocity",
        FROM basic_metrics_ethereum b
        LEFT JOIN market_cap m ON b.Date = m.Date
        LEFT JOIN nvt_ratio n ON b.Date = n.Date
        LEFT JOIN velocity v ON b.Date = v.Date
    '''
    conn.execute(join_result_table_query)

    # Upload blocks_mined table to S3
    query = "SELECT * FROM computed_metrics_ethereum"
    conn.execute(query)
    computed_metrics_df = conn.fetchdf()
    # append df to transactions_data_df
    print("Converting to parquet")
    table = pa.Table.from_pandas(computed_metrics_df)
    with io.BytesIO() as parquet_buffer:
        pq.write_table(table, parquet_buffer)  # Corrected Parquet write function
        # upload to s3
        print("Uploading transaction_data.parquet to S3")
        s3_client.put_object(Bucket= bucket, Key= "Ethereum/charts_computed.parquet", Body=parquet_buffer.getvalue())

    conn.close()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }