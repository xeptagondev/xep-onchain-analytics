from selenium.webdriver.common.by import By
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
from selenium import webdriver
import pandas as pd
import boto3

#Downloading Basic metrics from Blockchair   
#metrics_desc:Metrics List |  cwd:local machine or aws s3 | bucket & key: only for aws s3 | dir: download path 
def download(session, metrics_desc, cwd, bucket, key, dir):  
    s3 = session.resource('s3')  
    s3 = session.client('s3')
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    prefs = {'download.default_directory' : dir}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()

    #iterate through metrics list and download charts
    for charts in metrics_desc["chartsheets"]:
         driver.get("https://blockchair.com/bitcoin/charts/"+str(charts)+"")
         wait_time = 10
         download_tsv = WebDriverWait(driver, wait_time).until(
    EC.element_to_be_clickable((By.ID, 'download-tsv-button'))
)
         download_tsv.click()
         
         while not os.path.exists("data.tsv"):
             time.sleep(1)
         os.rename("data.tsv", ""+charts+".tsv")

    # Upload the file to S3 if running in aws s3
         if cwd.startswith("s3://"):
            with open(os.path.join(dir, f"{charts}.tsv"), 'rb') as data:
                s3.upload_fileobj(data, str(bucket), str(key)+f"{charts}.tsv")
                print("Successfully Downloded "+str(charts)+".tsv")
                os.remove(charts+".tsv")