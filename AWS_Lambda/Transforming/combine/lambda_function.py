import json
import boto3
import pyarrow.parquet as pq
import io
import duckdb as ddb
import pandas as pd
from botocore.exceptions import ClientError
import pyarrow as pa

# Read Config.json
with open("config.json") as json_file:
    config = json.load(json_file)


# Connect to S3 bucket
print("Connecting to S3 bucket")
session = boto3.Session(aws_access_key_id = config['ACCESS_KEY'], aws_secret_access_key = config['SECRET_KEY'])
s3_client = session.client('s3')
print("Connected to S3 bucket")

# docstring template
'''
    This function combines all the dataframes into one dataframe and writes it to a parquet file
    Parameters:
        None
    Returns:
        None
'''
def handler(event = None, context= None):
    conn = ddb.connect()

    charts_df = s3_client.get_object(Bucket="onchain-downloads", Key="Ethereum/charts_computed.parquet")
    charts_df = pq.read_table(io.BytesIO(charts_df['Body'].read())).to_pandas()
    blocks_mined_df = s3_client.get_object(Bucket="onchain-downloads", Key="Ethereum/blocks_mined.parquet")
    blocks_mined_df = pq.read_table(io.BytesIO(blocks_mined_df['Body'].read())).to_pandas()
    transactions_df = s3_client.get_object(Bucket="onchain-downloads", Key="Ethereum/transaction_data.parquet")
    transactions_df = pq.read_table(io.BytesIO(transactions_df['Body'].read())).to_pandas()

    join_result_table_query = '''
        CREATE OR REPLACE TABLE computed_metrics_ethereum AS 
        SELECT *
        FROM charts_df c
        LEFT JOIN blocks_mined_df b ON b.Date = c.Date
        LEFT JOIN transactions_df t ON t.Date = c.Date
    '''
    print("Joining tables")
    conn.execute(join_result_table_query)

    # Write to parquet file
    # Write to parquet file
    query = "SELECT * FROM computed_metrics_ethereum"
    result = conn.execute(query).fetchdf()
    # remove date:1 and date:2 columns
    result.drop(['Date:1', 'Date:2'], axis=1, inplace=True)
    result.sort_values(by=['Date'], inplace=True, ignore_index=True)
    result = pa.Table.from_pandas(result)
    with io.BytesIO() as parquet_buffer:
        pq.write_table(result, parquet_buffer)  # Corrected Parquet write function
        # upload to s3
        print("Uploading combined data to S3")
        s3_client.put_object(Bucket= "onchain-downloads", Key= "Ethereum/computed_data.parquet", Body=parquet_buffer.getvalue())


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

