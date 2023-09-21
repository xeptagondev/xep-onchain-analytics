import os
import pandas as pd
import glob
import boto3
import pandas as pd
import io
import gzip
def convert(session, cwd, bucket, file_path, file_name, conn):
    
    if cwd.startswith("s3://"):
        s3_object = session.client('s3')
        name = file_name.split('.')[0]
        #Reading Blockdata  files directly From AWS S3
        if name.startswith("block"):
            name = name.split('.')[0]
            s3 = boto3.resource("s3")
            obj = s3.Object(bucket, file_path)
            with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
                content = gzipfile.read()
            df = pd.read_csv(io.BytesIO(content), delimiter='\t', header=0)
        #Reading Blockchair files From s3
        else:
            get_file = s3_object.get_object(Bucket=bucket, Key=file_path)
            get = (get_file['Body'].read())
            df = pd.read_csv(io.BytesIO(get), delimiter='\t', header=0)
            df['Time'] = pd.to_datetime(df['Time'], format='%d.%m.%Y').dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
            df['Time'] = df['Time'].str[:-3] + 'Z'
        
        conn.register(name, df) 
       

    else:
        # query string sections
        str1 = "COPY (SELECT * FROM read_csv_auto('"
        str2 = "', delim='\t', header=True, sample_size=-1)) TO '"
        str3 = ".parquet'"
    
        path_index = os.listdir()
        # iterate through the paths that are directories (data files excl. *.duckdb files)    
        for path in path_index:
                files = glob.glob('*.tsv')         
                # iterate through each file
                for file in files:
                    name = file.split('.')[0]
                    try:
                        # try to execute the query using the read_csv_auto function
                        query = str1 + file + str2 + name + str3
                        conn.execute(query)
                    except RuntimeError:
                        # runtime error can be avoided by just reading the file using pandas which is slower
                        df = pd.read_csv(file, sep = '\t')
                        query = "COPY df TO '" + name + str3
                        conn.execute(query)
                    #remove the original .tsv file to save space
                    os.remove(file)
