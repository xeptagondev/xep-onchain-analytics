
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

Create a config.json file (replace the credentials keys, password and file path with your own):
```
{
    "ddb": {
        "database": "/filepath"
    },
    "postgre": {
        "engine": "postgresql://user:password@host:port/bitcoin",
        "database": "bitcoin",
        "host": "host",
        "user": "user",
        "password": "password",
        "port": "port"
    },
    "APIkey": {
        "blockdata": "APIKEY",
        "blockchair": "APIKEY",
        "bitcoinabuse": "APIKEY"
    },
    "init": {
        "bitcoinabuse": 2
    }
}

```

Change *start_date* and *end_date* parameters in `extract/main.py`

Run main.py:

```
python3 extract/main.py
```

Run `init_bitcoin_abuse_to_postgresql.py` and `load_BABD_to_postgresql.py`:

```
python3 anomaly_detection/init_bitcoin_abuse_to_postgresql.py
python3 anomaly_detection/load_BABD_to_postgresql.py
```
