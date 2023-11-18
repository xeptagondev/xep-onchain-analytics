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


def download(config):
    '''Iterates through each cryptocurrency to download their respective .tsv blockchair charts files specified under config's blockchair_metrics.'''
    cryptocurrencies = config['cryptocurrencies'] # get list of cryptocurrencies

    for crypto in cryptocurrencies:

        metrics_desc = config['blockchair_metrics'][crypto] # get list of charts metrics to download

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        print(f"curr work dir: {os.getcwd()}")
        if not os.path.exists(crypto):
            os.mkdir(crypto)
        os.chdir(crypto) # cd into crypto from data
        print(f"after changing dir (should be crypto name): {os.getcwd()}")

        download_dir = os.path.join(os.getcwd(), "basic_metrics")
        if not os.path.exists(download_dir):
            os.mkdir(download_dir)
        print(download_dir)
        # print(os.getcwd())

        chrome_options.add_experimental_option(
            "prefs", {"download.default_directory": download_dir})
        driver = webdriver.Chrome(service=Service(
            ChromeDriverManager().install()), options=chrome_options)

        # iterate through each metric for that cryptocurrency
        for charts in metrics_desc:
            driver.get(f"https://blockchair.com/{crypto}/charts/{charts}")
            wait_time = 10
            download_tsv = WebDriverWait(driver, wait_time).until(
                EC.element_to_be_clickable((By.ID, 'download-tsv-button'))
            )

            download_tsv.click()

            while not os.path.exists("basic_metrics/data.tsv"):
                time.sleep(1)
                print('sleep')
            os.rename("basic_metrics/data.tsv", f"basic_metrics/{charts}.tsv")
    os.chdir('../')
