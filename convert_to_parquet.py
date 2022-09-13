import duckdb as ddb
from datetime import date, timedelta

# Connecting to in-memory temporary database
conn = ddb.connect()

str1 = "COPY (SELECT * FROM read_csv_auto('blockchair_bitcoin_transactions_"
str2 = ".tsv', delim='\t', header=True, sample_size=-1)) TO 'transaction_"
str3 = ".parquet'"

# For 20190826: RuntimeError: basic_string::replace: __pos (which is 18446744073709478581) > this->size() (which is 9)
# For 20210524: RuntimeError: basic_string::replace: __pos (which is 18446744073709419013) > this->size() (which is 9)
start_date = date(2021, 5, 25)
end_date = date(2021, 5, 31)    

delta = end_date - start_date   # returns timedelta

for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    query = str1 + day.strftime('%Y%m%d') + str2 + day.strftime('%Y%m%d') + str3
    conn.execute(query)

# query = "COPY (SELECT * FROM read_csv_auto('blockchair_bitcoin_transactions_202003*.tsv', delim='\t', header=True, sample_size=-1)) TO 'transactions_202003.parquet'"

print("Converted")
