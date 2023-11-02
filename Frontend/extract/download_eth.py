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

def download_eth():
    metrics_desc = pd.read_csv('chartsheet_eth.csv')

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage') 

    download_main_dir = os.path.join(os.getcwd(), 'data', 'basic_metrics', 'Ethereum')
    if not os.path.exists(download_main_dir):
        os.makedirs(download_main_dir)
    os.chdir(download_main_dir)
    dir = os.path.join(download_main_dir,str(date.today()))
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    print(f"current dir: {os.getcwd()}")

    chrome_options.add_experimental_option("prefs", {"download.default_directory": dir})
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    for charts in metrics_desc["chartsheets"]:
        driver.get(f"https://blockchair.com/ethereum/charts/{charts}")
        wait_time = 10
        download_tsv = WebDriverWait(driver, wait_time).until(
    EC.element_to_be_clickable((By.ID, 'download-tsv-button'))
        )

        download_tsv.click()

        while not os.path.exists("data.tsv"):
             time.sleep(1)
        os.rename("data.tsv", f"{charts}.tsv")

download_eth()