import json
import boto3
import pyarrow.parquet as pq
import io
import duckdb as ddb
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
import pyarrow as pa

# Read Config.json
with open("config.json") as json_file:
    config = json.load(json_file)


# Connect to S3 bucket
print("Connecting to S3 bucket")
session = boto3.Session(aws_access_key_id = config['ACCESS_KEY'], aws_secret_access_key = config['SECRET_KEY'])
s3_client = session.client('s3')
blockchair_key = config['BLOCKCHAIR_KEY']
num_files = config.get("NUM_FILES", 100)
print("Connected to S3 bucket")

def handler(event = None, context= None):

    conn = ddb.connect()

    bucket = "onchain-downloads"
    prefix = "Ethereum-Raw/transactions/"
    prefix_1 = "Ethereum/"

    # try to find parquet file with clean data
    df = find_existing_file(s3_client, bucket, prefix_1)
    if df is None:
        existing_dates = []
        last_value = ""
        objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix)

    else:
        # sort existing dates
        existing_dates = df["Date"]
        existing_dates = existing_dates.sort_values().tolist()
        print("Existing dates: ", existing_dates)
        # last value
        last_value = existing_dates[-1]
        print("Last value: ", last_value)
        objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Marker= "Ethereum-Raw/transactions/blockchair_ethereum_transactions_" + last_value.strftime("%Y%m%d") + ".parquet")
    
    # Create temporary table to store data
    create_table = '''
        CREATE TEMP TABLE transactions_data (Date TIMESTAMP, "Active Addresses" INTEGER, "Active Sending Addresses" INTEGER, "Active Receiving Addresses" INTEGER,
        "Transaction Count ($0.00 - $1.00)" INTEGER,
        "Transaction Count ($1.00 - $10.00)" INTEGER,
        "Transaction Count ($10.00 - $100.00)" INTEGER,
        "Transaction Count ($100.00 - $1K)" INTEGER,
        "Transaction Count ($1K - $10K)" INTEGER,
        "Transaction Count ($10K - $100K)" INTEGER,
        "Transaction Count ($100K and above)" INTEGER,
        "Transaction Count (Total)" INTEGER)
    '''
    conn.execute(create_table)
    # Process parquet files for blocks data
    print("Starting to process files")
    counter = 0
    # Loop for all parquet files
    for i in objects.get('Contents'):
        if counter >= num_files:
            break
        counter += 1
        if i.get('Key') == "Ethereum-Raw/blocks/log.txt":
            print("Only log left")
            break
        # read current object
        s3_object = s3_client.get_object(Bucket="onchain-downloads", Key=i.get('Key'))
        day = i.get('Key').split('/')[2].split('.')[0].split("_")[3]
        day = datetime.strptime(day, '%Y%m%d').date()
        day = datetime.combine(day, datetime.min.time())
        print("Processing file for ", day)

        transactions_df_original = pq.read_table(io.BytesIO(s3_object['Body'].read())).to_pandas()

        transactions_df = transactions_df_original[~transactions_df_original['type'].isin(['synthetic_coinbase'])][['index', 'type', 'sender', 'recipient']]
        all_unique_transactions_df = pd.concat([transactions_df['sender'],transactions_df['recipient']]).unique()
        total_active_addresses = len(all_unique_transactions_df)

        # Active Sending Addresses
        unique_sending_transactions_df = transactions_df['sender'].unique()
        total_active_sending_addresses = len(unique_sending_transactions_df)
        
        # Active Receiving Addresses
        unique_receiving_transactions_df = transactions_df['recipient'].unique()
        total_active_receiving_addresses = len(unique_receiving_transactions_df)

        # Transaction Count
        transactions_df_type_filtered = transactions_df_original[~transactions_df_original['type'].isin(['synthetic_coinbase'])]
        calculate_num_large_t = '''
        SELECT COUNT(CASE WHEN value_usd >= 0 AND value_usd < 1 THEN hash END), COUNT(CASE WHEN value_usd >= 1 AND value_usd < 10 THEN hash END), 
        COUNT(CASE WHEN value_usd >= 10 AND value_usd < 100 THEN hash END), COUNT(CASE WHEN value_usd >= 100 AND value_usd < 1000 THEN hash END), 
        COUNT(CASE WHEN value_usd >= 1000 AND value_usd < 10000 THEN hash END), COUNT(CASE WHEN value_usd >= 10000 AND value_usd < 100000 THEN hash END), 
        COUNT(CASE WHEN value_usd >= 100000 THEN hash END), COUNT(hash) FROM transactions_df_type_filtered 
        '''
        num_large_t_value = conn.execute(calculate_num_large_t).fetchone()

        conn.execute('INSERT INTO transactions_data VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?)', 
                    [day, total_active_addresses, total_active_sending_addresses, total_active_receiving_addresses, 
                     num_large_t_value[0], num_large_t_value[1], num_large_t_value[2], num_large_t_value[3], 
                     num_large_t_value[4], num_large_t_value[5], num_large_t_value[6], num_large_t_value[7]])

    # Upload blocks_mined table to S3
    query = "SELECT * FROM transactions_data"
    conn.execute(query)
    transactions_data_df = conn.fetchdf()
    # append df to transactions_data_df
    transactions_data_df = pd.concat([df, transactions_data_df], ignore_index=True)
    transactions_data_df = transactions_data_df.sort_values(by=['Date'])
    print("Transactions data df: ", transactions_data_df)
    # df to parquet
    print("Converting to parquet")
    table = pa.Table.from_pandas(transactions_data_df)
    with io.BytesIO() as parquet_buffer:
        pq.write_table(table, parquet_buffer)  # Corrected Parquet write function
        # upload to s3
        print("Uploading transaction_data.parquet to S3")
        s3_client.put_object(Bucket= bucket, Key= "Ethereum/transaction_data.parquet", Body=parquet_buffer.getvalue())

    conn.close()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def find_existing_file(s3_client, bucket, prefix):
    print("Finding existing file")
    try:
        s3_response = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum/transaction_data.parquet")
        print("Found existing file")
        transactions_data_df = pq.read_table(io.BytesIO(s3_response['Body'].read())).to_pandas()
    except ClientError as ex:
        print("No file found")
        return None

    return transactions_data_df