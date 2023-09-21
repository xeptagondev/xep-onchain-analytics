from selenium.webdriver.common.by import By
import os
import time
from selenium import webdriver

def download():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage') 
    chrome_options.add_experimental_option("prefs", {"download.default_directory":r"/xep-onchain-analytics/data/basic_metrics"})
    driver = webdriver.Chrome('chromedriver', options=chrome_options)

    # Download price
    driver.get("https://blockchair.com/bitcoin/charts/price")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/price.tsv")

    # Average Transaction Value in USD
    driver.get("https://blockchair.com/bitcoin/charts/average-transaction-amount-usd")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/average_transaction_value.tsv")

    # Coin Days Destroyed
    driver.get("https://blockchair.com/bitcoin/charts/coindays-destroyed")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/cdd.tsv")

    # Circulating Supply
    driver.get("https://blockchair.com/bitcoin/charts/circulation")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/circulating_supply.tsv")

    # Cost per Transaction
    driver.get("https://blockchair.com/bitcoin/charts/average-transaction-fee-usd")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/average_transaction_fee.tsv")

    # Difficulty
    driver.get("https://blockchair.com/bitcoin/charts/difficulty")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/difficulty.tsv")

    # Transaction Count
    driver.get("https://blockchair.com/bitcoin/charts/transaction-count")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/transaction_count.tsv")

    # Transaction Volume in BTC
    driver.get("https://blockchair.com/bitcoin/charts/transaction-volume")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/transaction_volume_btc.tsv")

    # Transaction Volume in USD
    driver.get("https://blockchair.com/bitcoin/charts/transaction-volume-usd")
    download_tsv = driver.find_element(By.ID, "download-tsv-button").click()

    while not os.path.exists("basic_metrics/data.tsv"):
        time.sleep(1)
    os.rename("basic_metrics/data.tsv", "basic_metrics/transaction_volume_usd.tsv")

