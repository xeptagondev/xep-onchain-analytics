import requests
import json
import pandas as pd

#retrieve bitcoin blockchain stats from blockchair API
stats_cont = requests.get("https://api.blockchair.com/bitcoin/stats").text
jsondata = json.loads(stats_cont)

#store stats in a dataframe
bitcoin_stats = pd.DataFrame([jsondata["data"]])

#add time & date of recent update into stats dataframe
last_update = jsondata["context"]["cache"]["until"]
bitcoin_stats['last_update'] = last_update


