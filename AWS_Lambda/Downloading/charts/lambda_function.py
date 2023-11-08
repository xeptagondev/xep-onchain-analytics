from selenium.webdriver.common.by import By
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date 
import pandas as pd
import json
from datetime import datetime
import boto3
# import pandas as pd

# Read Config.json
with open("config.json") as json_file:
    config = json.load(json_file)

# Connect to S3 bucket
print("Connecting to S3 bucket")
session = boto3.Session(aws_access_key_id = config['ACCESS_KEY'], aws_secret_access_key = config['SECRET_KEY'])
s3_client = session.client('s3')
metrics = config["metrics_desc"]
print("Connected to S3 bucket")

def handler(event = None, context= None):
    bucket_name = "onchain-downloads"
    # Get today's date time
    today = datetime.now().date()
    
    print("Starting to download ETH metrics now!")

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage') 
    service = webdriver.ChromeService(executable_path="/usr/local/bin/chromedriver")

    chrome_options.add_experimental_option("prefs", {"download.default_directory": "/tmp"})
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(e)
        print("Chrome driver failed to start")
        return
    driver.maximize_window()

    print("driver created")

    # download from Blockchair
    for charts in metrics:
        driver.get(f"https://blockchair.com/ethereum/charts/{charts}")
        print(f"navigating to webpage for {driver.title}")
        wait_time = 20
        download_tsv = WebDriverWait(driver, wait_time).until(
            EC.element_to_be_clickable((By.ID, 'download-tsv-button'))
        )

        download_tsv.click()

        print("clicked on download tsv button")

        while not os.path.exists("data.tsv"):
             time.sleep(1)
        os.rename("data.tsv", f"{charts}.tsv")
        print("downloaded file!")

        # upload to S3
        print("Starting upload to S3")
        s3_client.upload_file(f"/tmp/{charts}.tsv", bucket_name, f"Ethereum-Raw/charts/{charts}.tsv")

    s3_client.put_object(Bucket = bucket_name, Key = "Ethereum-Raw/charts/log.txt", 
                         Body = str({"lastest_function_run" : str(today)}))

    print("Done downloading all ETH basic metrics from blockchair!")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
