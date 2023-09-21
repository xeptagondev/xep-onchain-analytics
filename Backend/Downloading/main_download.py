from Downloading.blockdata_download import scrape
from Downloading.blockchair_download import download
from Downloading.db_connect import db_connect
import os
from datetime import date, timedelta 
import datetime
import pandas as pd
import boto3
def downloading(session, cwd, metrics_desc):   
    
    #Connecting to postgres database
    engine, psqlconn = db_connect()
    qry = """SELECT "Date" FROM computed_metrics ORDER BY "Date" DESC LIMIT 1 """
    # Datetime parameters
    format = '%Y-%m-%d'
    # Datetime parameters
    try:
        qry = """SELECT "Date" FROM computed_metrics ORDER BY "Date" DESC LIMIT 1 """
        s_date = pd.read_sql(qry, psqlconn)

        start_date = s_date.iloc[0]["Date"].strftime("%Y-%m-%d")
        start_date = datetime.datetime.strptime(str(start_date),format).date()+ timedelta(days = 1) 
    except Exception as e:
        start_date = '2009-01-12'
        start_date = datetime.datetime.strptime(str(start_date),format).date()
    end_date = date.today() - timedelta(days = 2)
     #Setting Path for Downloads & Creating Folder
    if cwd.startswith("s3://"):
        s3 = session.resource('s3')
        # s3 = boto3.resource('s3')
        bucket, path = cwd.replace("s3://", "").split("/", 1)
        key = path+ str(date.today())+"/"
        print(key)
        s3.Bucket(bucket).put_object(Key = key)
        cwd = "s3://"+bucket+"/"+key
        print(cwd)
        dir = os.getcwd()
    else:
        main_dir =os.path.join(cwd,'Downloads')
        if not os.path.exists(main_dir):
            os.mkdir(main_dir)
        os.chdir(main_dir)
        dir = os.path.join(main_dir,str(date.today()))
        if not os.path.exists(dir):
            os.mkdir(dir)
        os.chdir(dir)
        key = None
        bucket = None 
    
   
    #Downloading Sources
    print("Start Downloading Blockchair............")
    try:
        download(session, metrics_desc, cwd, bucket, key, dir)
        print("Successfullly Blockchair Donwload Complete.............................")
    except Exception as e:
        print(e,"Wait trying again ............")
        if cwd.startswith("s3://"):
            download(session, metrics_desc, cwd, bucket, key, dir)
        else:
            err_files = os.listdir()
            for err in err_files:
                os.remove(err)
            download(session, metrics_desc, cwd, bucket, key, dir)
        pass

    print("Start Downloading BlockData.............")
    try:
        scrape(session, start_date, end_date, str(cwd), bucket, key)
        print("Successfully BlockData Download Complete........")
    except Exception as e:
        print(e,"Downloading Pause for Today")
        pass
    
