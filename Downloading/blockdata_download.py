from datetime import timedelta
import wget
import os
from datetime import timedelta
from datetime import date, timedelta
import gzip, shutil
import boto3
import io
import zipfile
import pandas as pd
def scrape(session, start_date, end_date, cwd, bucket, key):
    s3 = session.resource('s3')
    s3 = session.client("s3")
    delta = end_date - start_date   # returns timedelta
    # path_index to keep track of the different paths in our file directory and the url path
    path_index = [
        'inputs', 
        'outputs'
        # 'transactions'
        ]

    # metric_index to keep track of the different metrics
    metric_index = [
        '/blockchair_bitcoin_inputs_', 
        '/blockchair_bitcoin_outputs_'
        # '/blockchair_bitcoin_transactions_'
        ]

    # sectioned urls
    url1 = "http://blockdata.loyce.club/"
    url2 = ".tsv.gz?key=" + 'APIkey'
    
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
            s3_name = metric + day.strftime('%Y%m%d') +'.tsv.gz'
            # append file name to url sections
            url = url1 + filename + url2
            
            # download file to aws s3 or local machine
            if cwd.startswith("s3://"):
                os.system(' wget --no-check-certificate -O '+str(s3_name)+' '+str(url)+'')
                          
                os.system(' aws s3 cp '+str(s3_name)+' {}'.format(cwd) )
                #rename the file
                '''
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key+'-'}, Key=f'{key[:-1]}{s3_name}')
                s3.delete_object(Bucket=bucket, Key=key+'-')
                '''

            else:
                wget.download(url, out = cwd)

                # since file is in tsv.gz, unzip file
                gzzip = cwd+ metric + day.strftime('%Y%m%d') + '.tsv.gz'
                unzip = (os.path.basename(gzzip)).rsplit('.',1)[0]
                with gzip.open(gzzip,"rb") as f_in, open(unzip,"wb") as f_out:
                  shutil.copyfileobj(f_in, f_out)
                os.remove(gzzip)

