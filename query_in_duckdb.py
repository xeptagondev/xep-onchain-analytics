import duckdb as ddb
import pandas as pd
import pyarrow as pa
import numpy as np
from pyarrow import csv
import pyarrow.parquet as pq
import dask.dataframe as dd

# Connecting to in-memory temporary database
conn = ddb.connect()

df = dd.read_parquet('transactions_201907.parquet')
print(df.columns)

# Print 5 rows from data set
# conn.execute("EXPLAIN ANALYZE SELECT * FROM 'transactions_201907.parquet' ORDER BY cdd_total DESC LIMIT 5")
# 18.18s to run this query

# Print top 10 transactions with highest input
# conn.execute("EXPLAIN ANALYZE SELECT * FROM 'transactions_201907.parquet' ORDER BY input_total DESC LIMIT 10")
#19.14s

# Print unique block ids
# conn.execute("EXPLAIN ANALYZE SELECT DISTINCT block_id FROM 'transactions_201907.parquet'")
#0.262s

# Print group by query
# conn.execute("EXPLAIN ANALYZE SELECT block_id, SUM(input_total) FROM 'transactions_201907.parquet' GROUP BY block_id")
#0.844s

# Querying multiple parquet files
# conn.execute("EXPLAIN ANALYZE SELECT time, SUM(input_total) FROM '*.parquet' GROUP BY time LIMIT 5")
#23.71s

# Load Parquet File into Table
create_table = """CREATE TABLE transactions AS SELECT * FROM 'transaction_20190703.parquet';"""
conn.execute(create_table)

create_index = """CREATE INDEX idx ON transactions(time);"""
conn.execute(create_index)

query = """EXPLAIN ANALYZE SELECT sum(size), sum(output_total_usd) FROM transactions;"""
conn.execute(query)

# Print results of query
result = conn.fetchall()
for x in result:
  print(x)