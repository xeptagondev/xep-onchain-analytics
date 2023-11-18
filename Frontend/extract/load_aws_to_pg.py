import boto3
import pyarrow.parquet as pq
import os


def load_aws_data_to_pg(config, engine):
    '''Fetches basic and computed metrics data from AWS S3 bucket and loads it to local postgres database.'''
    session = boto3.Session(
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'], aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])
    s3_client = session.client('s3')

    aws_crypto_folders = config['AWS']['crypto_folders']

    for crypto in aws_crypto_folders:
        aws_data_files = config["AWS"]["data_files"][crypto]

        for file in aws_data_files:
            s3_client.download_file(
                'onchain-downloads', f'{crypto}/{file}', file)

            read_file = pq.read_table(file).to_pandas()

            pg_table_names = config["pg_table_names"]

            if "basic" in file:
                table_name = pg_table_names["basic_metrics"][crypto.lower()]
            else:
                table_name = pg_table_names["computed_metrics"][crypto.lower()]

            read_file.to_sql(table_name, engine, index=False,
                             if_exists='replace')

            # remove file from local system after loading
            os.system("rm " + file)
            # os.system("del " + file)  # use this for windows
