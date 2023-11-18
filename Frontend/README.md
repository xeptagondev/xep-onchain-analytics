
# Xeptagon Blockchain Analytics
An opensource framework for on-chain analytics.

## How to run this app

```
git clone https://github.com/xeptagondev/xep-onchain-analytics.git
cd xep-onchain-analytics/webapp
```
Install all required packages by running:
```
pip install -r requirements.txt
```

Run this app locally with:
```
python3 application.py
```

And visit:
```
http://127.0.0.1:8050/
```

## To deploy app on AWS Elastic Beanstalk

Create an instance of the web on AWS:
```
eb create
```

To update the app after making a change:
```
eb deploy
```

To open application URL in browser:
```
eb open
```

To delete app from AWS Cloud:
```
eb terminate
```
## To initialize database

Create a `config.json` file (replace the credentials keys, password and file path with your own):
```
{
    "ddb": {
        "database": "data.db"
    },
    "postgre_webapp": {
        "engine": "postgresql://postgres:YourPassword@YourHost:5432/YourDatabase",
        "database": "YourDatabase",
        "host": "YourHost",
        "user": "postgres",
        "password": "YourPassword",
        "port": "5432"
    },
    "postgre_extract": {
        "engine": "postgresql://postgres:YourPassword@localhost:5432/YourDatabase",
        "database": "YourDatabase",
        "host": "localhost",
        "user": "postgres",
        "password": "YourPassword",
        "port": "5432"
    },
    "APIkey": {
        "bitcoin": {"blockdata": "APIKEY", "blockchair": "APIKEY", "bitcoinabuse": "APIKEY"},
        "ethereum": {"blockchair": "APIKEY"}
    },
    "init": {
        "bitcoinabuse": 2
    },
    "blockchair_metrics": {
        "bitcoin": [
            "price",
            "average-transaction-amount-usd",
            "coindays-destroyed",
            "circulation",
            "average-transaction-fee-usd",
            "difficulty",
            "transaction-count",
            "transaction-volume",
            "transaction-volume-usd"
        ],
        "ethereum": [ 
            "average-block-size",
            "average-gas-used",
            "average-gas-price",
            "number-of-new-contracts",
            "circulation",
            "transaction-volume-usd",
            "price",
            "average-transaction-amount-usd",
            "average-transaction-fee-usd",
            "average-block-interval",
            "average-gas-limit",
            "erc-20-transfers"
        ]
    },
    "scrape": {
        "path_index": {"bitcoin": ["inputs", "outputs", "transactions"], "ethereum": ["blocks", "transactions"]},
        "metric_index": {"bitcoin": ["/blockchair_bitcoin_inputs_", "/blockchair_bitcoin_outputs_", "/blockchair_bitcoin_transactions_"],
                         "ethereum": ["/blockchair_ethereum_blocks_", "/blockchair_ethereum_transactions_"]    
                        },
        "urls": {"bitcoin": "http://blockdata.loyce.club/", "ethereum": "https://gz.blockchair.com/ethereum/"}

    },
    "cryptocurrencies": ["bitcoin", "ethereum"],
    "pg_table_names": {
        "basic_metrics": {"bitcoin": "basic_metrics", "ethereum": "basic_metrics_ethereum"},
        "computed_metrics": {"bitcoin": "computed_metrics", "ethereum": "computed_metrics_ethereum"}
    },
    "AWS": {
        "AWS_ACCESS_KEY_ID":"APIKEY",
        "AWS_SECRET_ACCESS_KEY":"APIKEY",
        "crypto_folders": ["Bitcoin", "Ethereum"],
        "data_files": {
            "Bitcoin": [],
            "Ethereum": ["basic_metrics_ethereum.parquet", "computed_data.parquet"] 
        }
    }
}

```

Download the *BABD-13.csv* dataset from the link below and store it within a folder named `bitcoin_abuse` under `anomaly_detection`.

```
https://www.kaggle.com/datasets/lemonx/babd13
```

Change *start_date* and *end_date* parameters in `extract/main.py`

Run main.py using the following command:

```
python3 extract/main.py
```

Run `init_bitcoin_abuse_to_postgresql.py` and `load_BABD_to_postgresql.py`:

```
python3 anomaly_detection/init_bitcoin_abuse_to_postgresql.py
python3 anomaly_detection/load_BABD_to_postgresql.py
```
