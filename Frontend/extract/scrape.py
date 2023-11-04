from datetime import timedelta
import wget
import os

def scrape(start_date, end_date, config):
    delta = end_date - start_date   # returns timedelta

    cryptocurrencies = config['cryptocurrencies']

    for crypto in cryptocurrencies:
        print(f"curr work dir: {os.getcwd()}")
        if not os.path.exists(crypto):
            os.mkdir(crypto)
        os.chdir(crypto)
        print(f"after changing dir (should be crypto name): {os.getcwd()}")
        
        # path_index to keep track of the different paths in our file directory and the url path
        # path_index = [
        #     'inputs', 
        #     'outputs', 
        #     'transactions'
        #     ]
        path_index = config['scrape']['path_index'][crypto]

        # metric_index to keep track of the different metrics
        # metric_index = [
        #     '/blockchair_bitcoin_inputs_', 
        #     '/blockchair_bitcoin_outputs_', 
        #     '/blockchair_bitcoin_transactions_'
        #     ]

        metric_index = config['scrape']['metric_index'][crypto]

        # sectioned urls
        # url1 = "http://blockdata.loyce.club/"
        url1 = config['scrape']['urls'][crypto]
        url2 = ".tsv.gz?key=" + config['APIkey']['blockchair'][crypto]
        
        # iterate through each metric (input/output/transaction)
        for i in range(len(path_index)):
            # instantiate the download path and metric
            path = path_index[i]
            if not os.path.exists(os.path.join(os.getcwd(),path)):
                os.mkdir(os.path.join(os.getcwd(),path))
            print(f"curr path index: {path}")
            print(f"download path: {path}")
            metric = metric_index[i]
            print(f"curr metric:{metric}")
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
        os.chdir('../')
