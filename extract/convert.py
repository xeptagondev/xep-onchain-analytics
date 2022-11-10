import os
import duckdb as ddb
import glob
import pandas as pd

def convert():
    # query string sections
    str1 = "COPY (SELECT * FROM read_csv_auto('"
    str2 = "', delim='\t', header=True, sample_size=-1)) TO '"
    str3 = ".parquet'"

    conn = ddb.connect()

    path_index = os.listdir()

    # iterate through the paths that are directories (data files excl. *.duckdb files)    
    for path in path_index:
        if os.path.isdir(path):
            os.chdir(path)
            # get all tsv files in the directory
            files = glob.glob('*.tsv')
            print(files)
            
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
                os.system("rm " + file)
            os.chdir('../')
