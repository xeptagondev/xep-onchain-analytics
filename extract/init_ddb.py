import os
from datetime import timedelta
def init_ddb(start_date, end_date, conn):
    delta = end_date - start_date

    table_bool = conn.execute("""select count(*) from information_schema.tables where table_name = 'test_input'""")

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        if i == 0 and table_bool.fetchone()[0] == 0:
            create_input_table = "CREATE OR REPLACE TABLE test_input AS SELECT * from 'inputs/blockchair_bitcoin_inputs_" + day.strftime('%Y%m%d') + ".parquet'"
            conn.execute(create_input_table)
            create_output_table = "CREATE OR REPLACE TABLE test_output AS SELECT * from 'outputs/blockchair_bitcoin_outputs_" + day.strftime('%Y%m%d') + ".parquet'"
            conn.execute(create_output_table)
            create_transactions_table = "CREATE OR REPLACE TABLE test_transactions AS SELECT * from 'transactions/blockchair_bitcoin_transactions_" + day.strftime('%Y%m%d') + ".parquet'"
            conn.execute(create_transactions_table)
        else:
            # loop here
            insert_input_table = "INSERT INTO test_input SELECT * from 'inputs/blockchair_bitcoin_inputs_" + day.strftime('%Y%m%d') + ".parquet'"
            conn.execute(insert_input_table)

            insert_output_table = "INSERT INTO test_output SELECT * from 'outputs/blockchair_bitcoin_outputs_" + day.strftime('%Y%m%d') + ".parquet'"
            conn.execute(insert_output_table)

            insert_transactions_table = "INSERT INTO test_transactions SELECT * from 'transactions/blockchair_bitcoin_transactions_" + day.strftime('%Y%m%d') + ".parquet'"
            conn.execute(insert_transactions_table)

