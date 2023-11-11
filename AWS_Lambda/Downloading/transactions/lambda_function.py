import json
from bs4 import BeautifulSoup
import wget
import gzip
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import ast 
import requests
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import duckdb

# Read Config.json
with open("config.json") as json_file:
    config = json.load(json_file)

# Connect to S3 bucket
print("Connecting to S3 bucket")
session = boto3.Session(aws_access_key_id = config['ACCESS_KEY'], aws_secret_access_key = config['SECRET_KEY'])
s3_client = session.client('s3')
blockchair_key = config['BLOCKCHAIR_KEY']
data_type = config["DATA_TYPE"]
num_files = config["NUM_FILES"]
print("Connected to S3 bucket")
con = duckdb.connect(database=':memory:')

def handler(event = None, context= None):
    bucket_name = "onchain-downloads"
    # Get today's date time
    today = datetime.now().date()
    
    # Check log in S3 Bucket
    update_information = get_information_from_log_file()

    # Check S3 bucket if it is updated
    # If it is updated, don't do anything
    if update_information["last_data_uploaded"] == str(today):
        return
    # If it is not updated, extract the data from one day after last uploaded date

    # Remove folders that are already in S3
    folders = get_folders_name(data_type)
    to_remove = []
    for folder in folders:
        date_of_folder = folder.split("_")[3].split(".")[0]
        date_of_folder = datetime(int(date_of_folder[0:4]), int(date_of_folder[4:6]), int(date_of_folder[6:8]))
        if date_of_folder <= update_information["last_data_uploaded"]:
            to_remove.append(folder)
        if date_of_folder > update_information["last_data_uploaded"]:
            break

    for folder in to_remove:
        folders.remove(folder)
    # For each folder, get the data, unzip folder then save it to S3   

    last_uploaded = download_from_blockchair(folders, blockchair_key, num_files)
    print("Updating log file in S3 Bucket")
    last_data_uploaded = clean_date(last_uploaded.split("_")[3].split(".")[0])
    s3_client.put_object(Bucket = bucket_name, Key = "Ethereum-Raw/" + data_type + "/log.txt", 
                         Body = str({"lastest_function_run" : str(today), "last_data_uploaded" : last_data_uploaded}))
    print("Updated log file in S3 Bucket")
    con.close()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



def download_from_blockchair(folders, blockchair_key = blockchair_key, number_of_files = num_files, data_type = data_type, s3_client = s3_client, conn = con):
    print("Starting download")
    bucket_name = "onchain-downloads"
    for _ in range(number_of_files):
        folder = folders.pop(0)
        print("Downloading " + folder)
        folder_name = folder.split(".")[0]
        # Download the folder
        try:
            downloaded_file = wget.download("https://gz.blockchair.com/ethereum/" + data_type + "/" + folder + "?key=" + blockchair_key, "/tmp/"+ folder)
        except Exception as e:
            print("Error downloading " + folder)
            print(e)
            break
        print("Downloaded " + folder)
        with gzip.open(downloaded_file, 'rb') as unzipped_file:
            print("Reading " + folder)
            df = pd.read_csv(unzipped_file, sep='\t')
            df["fee"] = df["fee"].astype(str) # Change fee to string to avoid error (Integer overflow)
            try:
                table = pa.Table.from_pandas(df)
            except Exception as e:
                print("Error reading " + folder)
                print(e)
                break
        with io.BytesIO() as parquet_buffer:
            pq.write_table(table, parquet_buffer)  # Corrected Parquet write function
            print("Uploading to S3 " + folder)
            s3_client.put_object(Bucket= bucket_name, Key= "Ethereum-Raw/" + data_type + "/" + folder_name + ".parquet", Body=parquet_buffer.getvalue())

    print("Finished downloading")
    return folder




def get_information_from_log_file(s3_client = s3_client, data_type = data_type):
    try:
        print("Getting information from log file in S3 Bucket")
        s3_response = s3_client.get_object(Bucket = "onchain-downloads", Key = "Ethereum-Raw/"+ data_type +"/log.txt")
        file_data = s3_response["Body"].read().decode("utf")
        update_information = ast.literal_eval(file_data) # {"lastest_function_run" : "2020-01-01", "last_data_uploaded" : "2020-01-01"}
        update_information = string_to_date(update_information)
        print("Got information from log file in S3 Bucket")
    except ClientError as ex:
        print("There is no log file")
        if ex.response['Error']['Code'] == 'NoSuchKey':
            update_information = {"lastest_function_run" : datetime(2022,1,1), "last_data_uploaded" : datetime(2020,1,1)}
        else:
            raise

    return update_information


def clean_date(date):
    return date[0:4] + "-" + date[4:6] + "-" + date[6:8]

def string_to_date(date_dict):
    for i in date_dict:
        print(date_dict[i])
        date_dict[i] = datetime.strptime(date_dict[i], "%Y-%m-%d")
    return date_dict

def get_folders_name(data_type):
    # Get all the folder names from website
    # Return a list of folders
    print("Scraping blockchair " + data_type)
    blockchair_content = requests.get("https://gz.blockchair.com/ethereum/" + data_type + "/").text
    soup = BeautifulSoup(blockchair_content, features="html.parser")
    all_folders = []

    for text in soup.find_all("a"):
        if "tsv.gz" in text.get("href"):
            all_folders.append(text.get("href"))

    return all_folders
