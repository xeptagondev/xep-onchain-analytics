import json
import boto3
import pyarrow.parquet as pq
import io
import duckdb as ddb
import pandas as pd
from datetime import datetime, timedelta
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
data_type = config["DATA_TYPE"]
print("Connected to S3 bucket")

def handler(event = None, context= None):
    '''
    Return success if the function is run successfully

            Parameters:
                    None

            Returns:
                    None

            Logic:
                    1. Find parquet file with clean data
                    2. If no parquet file found, get all parquet files from S3 blocks folder
                    3. If parquet file found, get all parquet files from S3 blocks folder after the last date in the parquet file
                    4. Create temporary table to store data
                    5. Process parquet files for blocks data
                    6. Upload blocks_mined table to S3

    '''
    conn = ddb.connect()

    bucket = "onchain-downloads"
    prefix = "Ethereum-Raw/blocks/"
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
        objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Marker= "Ethereum-Raw/blocks/blockchair_ethereum_blocks_" + last_value.strftime("%Y%m%d") + ".parquet")
    # Create temporary table to store data
    create_blocks_mined_table = '''
        CREATE TEMP TABLE blocks_mined (Date TIMESTAMP, "Blocks Mined" INTEGER)
    '''
    conn.execute(create_blocks_mined_table)
    # Process parquet files for blocks data
    print("Starting to process files")
    # Loop for all parquet files
    for i in objects.get('Contents'):
        if i.get('Key') == "Ethereum-Raw/blocks/log.txt":
            print("Only log left")
            break
        # read current object
        s3_object = s3_client.get_object(Bucket="onchain-downloads", Key=i.get('Key'))
        day = i.get('Key').split('/')[2].split('.')[0].split("_")[3]
        day = datetime.strptime(day, '%Y%m%d').date()
        if day in existing_dates:
            print("Caught by first")
            continue
        day = datetime.combine(day, datetime.min.time())
        if day in existing_dates:
            print("Caught by second")
            continue
        print("Processing file for ", day)
        blocks_df = pq.read_table(io.BytesIO(s3_object['Body'].read())).to_pandas()
        calculate_blocks_mined = "SELECT max(id) - min(id) + 1 FROM blocks_df"
        print("Calculating blocks mined")
        blocks_mined_value = conn.execute(calculate_blocks_mined).fetchone()
        print("Inserting into table")
        conn.execute('INSERT INTO blocks_mined VALUES (?, ?)', [day, blocks_mined_value[0]])

    # Upload blocks_mined table to S3
    query = "SELECT * FROM blocks_mined"
    conn.execute(query)
    blocks_mined_df = conn.fetchdf()
    # append df to blocks_mined_df
    blocks_mined_df = pd.concat([df, blocks_mined_df], ignore_index=True)
    blocks_mined_df = blocks_mined_df.sort_values(by=['Date'])
    print("Blocks mined df: ", blocks_mined_df)
    # df to parquet
    print("Converting to parquet")
    table = pa.Table.from_pandas(blocks_mined_df)
    with io.BytesIO() as parquet_buffer:
        pq.write_table(table, parquet_buffer)  # Corrected Parquet write function
        # upload to s3
        print("Uploading blocks_mined.parquet to S3")
        s3_client.put_object(Bucket= bucket, Key= "Ethereum/blocks_mined.parquet", Body=parquet_buffer.getvalue())

    conn.close()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def find_existing_file(s3_client, bucket, prefix):
    '''
    Find existing file that contains cleaned data in S3
    '''
    print("Finding existing file")
    try:
        s3_response = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum/blocks_mined.parquet")
        print("Found existing file")
        blocks_mined_df = pq.read_table(io.BytesIO(s3_response['Body'].read())).to_pandas()
    except ClientError as ex:
        print("No file found")
        return None

    return blocks_mined_df