from datetime import timedelta
import wget
import os

def scrape(start_date, end_date, config):
    delta = end_date - start_date   # returns timedelta

    # path_index to keep track of the different paths in our file directory and the url path
    path_index = [
        'inputs', 
        'outputs', 
        'transactions'
        ]

    # metric_index to keep track of the different metrics
    metric_index = [
        '/blockchair_bitcoin_inputs_', 
        '/blockchair_bitcoin_outputs_', 
        '/blockchair_bitcoin_transactions_'
        ]

    # sectioned urls
    url1 = "http://blockdata.loyce.club/"
    url2 = ".tsv.gz?key=" + config['APIkey']['blockdata']
    
    # iterate through each metric (input/output/transaction)
    for i in range(len(path_index)):
        # instantiate the download path and metric
        path = path_index[i]
        metric = metric_index[i]
        # iterate through the days between the start and end date
        for j in range(delta.days + 1):
            day = start_date + timedelta(days = j)
            # append path, metric, and date to get the file name
            filename = path + metric + day.strftime('%Y%m%d')

            # append file name to url sections
            url = url1 + filename + url2

            print(url)
            # download file to specified path
            wget.download(url, out = path)
            # since file is in tsv.gz, unzip file
            os.system('gunzip ' + filename + '.tsv.gz')
