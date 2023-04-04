from Computing.db_connect import db_connect
from Computing.convert import convert
from Computing.load_basic_metrics import load_basic_metrics
from Computing.load_get_realised_cap import get_realised_cap
from Computing.load_compute_metrics import compute
from Computing.load_to_pg import load_to_pg
import os
import datetime
import duckdb as ddb
import shutil
import boto3
from datetime import date, timedelta 

def computing(session, path):
# Database and Path configurations
    conn = ddb.connect()
    #Connecting to postgres database
    engine, psqlconn = db_connect()
    format = '%Y%m%d'
    
    if path.startswith("s3://"):
        s3 = session.resource('s3')
        bucket, Key = path.replace("s3://", "").split("/", 1)
        prefix = Key+ str(date.today())+"/"
        s3_bucket = s3.Bucket(bucket)
        dates = []
        #Read tsv files and Computation
        print("Start Adding Tables to DuckDB............")
        for obj in s3_bucket.objects.filter(Prefix=prefix):
            if obj.size == 0:
             continue
            file_full = obj.key
            file = file_full.split("/", 2)[2]
            if file.startswith("block"):
                    name = file.split('.')[0]
                    file_date =  name.split('_')[3]
                    dates.append(file_date)    
            convert(session, path, bucket, file_full, file, conn)
        print("Complete Adding Tables to DuckDB............")

        start_date = datetime.datetime.strptime(min(dates),format).date()     
        end_date = datetime.datetime.strptime(max(dates),format).date()
        
        print("Start Computing metrics............")
        load_basic_metrics(path, conn)
        get_realised_cap(path, start_date, end_date, conn)
        compute(path, start_date, end_date, conn)
        print("Complete Computing metrics............")
        print("Start loading data to SQL............")
        load_to_pg(engine, conn, start_date, end_date)
        print("Complete loading data to SQL............")
        
    else:
        
        complete_path = os.path.join(path,'Complete')
        if not os.path.exists(complete_path):
            os.mkdir(complete_path)
        os.chdir('Downloads')
        download_path = os.getcwd()
        # Datetime parameters
        path_index = os.listdir()
        dates = [] 
        for path in path_index:
            conn = ddb.connect()
            os.chdir(path)
            proccesing_path = os.getcwd()
            files = os.listdir()
            for file in files:
                if file.startswith("block"):
                    name = file.split('.')[0]
                    file_date =  name.split('_')[3]
                    dates.append(file_date)      
        
            #Start date and end date on download folder
            start_date = datetime.datetime.strptime(min(dates),format).date()     
            end_date = datetime.datetime.strptime(max(dates),format).date()   
               
            #Convert and Computation
            print("Start Converting tsv to PARQUET............")
            convert(path, bucket=None, file_path=None, file_name=None, conn=conn)
            print("Complete Converting tsv to PARQUET............")
            
            print("Start Computing metrics............")
            load_basic_metrics(path, conn)
            get_realised_cap(path,start_date, end_date, conn)
            compute(path,start_date, end_date, conn)
            print("Complete Computing metrics............")
           
            query = f'SELECT * FROM computed_metrics  '
            result = conn.execute(query).fetchall()
            print(result)
            print("Start loading data to SQL............")
            load_to_pg(engine, conn, start_date, end_date)
            print("Complete loading data to SQL............")
            
            os.chdir(download_path)
            print("Moving "+str(proccesing_path)+" to "+str(complete_path)+".............")
            shutil.move(path,complete_path)
            print("Folder Moved.................")
            
            dates.clear()
    conn.close()
    psqlconn.close()
