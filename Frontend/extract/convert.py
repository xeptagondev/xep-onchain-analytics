import os
import duckdb as ddb
import glob
import pandas as pd

def convert(config):
    '''Iterates through each cryptocurrency to convert all .tsv files to .parquet files before removing original .tsv files.'''
    print(os.getcwd())

    cryptocurrencies = config['cryptocurrencies'] # get list of cryptocurrencies

    # query string sections
    str1 = "COPY (SELECT * FROM read_csv_auto('"
    str2 = "', delim='\t', header=True, sample_size=-1)) TO '"
    str3 = ".parquet'"

    conn = ddb.connect()

    for crypto in cryptocurrencies:

        os.chdir(crypto) # cd into crypto from data

        path_index = os.listdir() # list all sub-directories

        # iterate through the paths that are directories (data files excl. *.duckdb files)    
        for path in path_index:
            if os.path.isdir(path):
                os.chdir(path)
                # get all tsv files in the directory
                files = glob.glob('*.tsv')
                print(os.getcwd())
                print(files)
                
                # iterate through each file
                for file in files:
                    name = file.split('.')[0]
                    print(f"filename: {name}")
                    try:
                        print('in try')
                        # try to execute the query using the read_csv_auto function
                        query = str1 + file + str2 + name + str3
                        print(f"query from duckdb: {query}")
                        conn.execute(query)
                        print('after executing query in try')
                    except RuntimeError:
                        # runtime error can be avoided by just reading the file using pandas which is slower
                        print('in except')
                        df = pd.read_csv(file, sep = '\t')
                        query = "COPY df TO '" + name + str3
                        print(f"query from pandas: {query}")
                        conn.execute(query)
                        print("after executing query in pandas")
                    #remove the original .tsv file to save space
                    os.system("rm " + file)
                os.chdir('../')
                print(os.getcwd())
        os.chdir('../')
        print(f"final dir: {os.getcwd()}")