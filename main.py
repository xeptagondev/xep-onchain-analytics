from Computing.main_compute import computing
from Downloading.main_download import downloading
import boto3
import os
import pandas as pd


cwd = os.getenv('PATH_VAR')
metrics_desc = pd.read_csv('chartsheet.csv')
if cwd.startswith('s3://'):
    session = boto3.Session( aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
else:
    cwd = os.getcwd()
    print("Local Path found.. ",cwd)

    session =None 
downloading(session, cwd, metrics_desc)
computing(session, cwd)