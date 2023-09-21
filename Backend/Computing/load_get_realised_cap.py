from datetime import timedelta

#Creating Realized Metric
def get_realised_cap(cwd, start_date, end_date, conn):
    if cwd.startswith('s3://'):
        delta = end_date - start_date   
        # Creates a realised_cap table if this is the first time the code is being run - instantiates the table
        conn.execute("""CREATE TABLE IF NOT EXISTS realised_cap (Date VARCHAR, "Realised Cap USD" DOUBLE, "Realised Cap BTC" DOUBLE)""")
        
        # Iterates through the timedelta
        for i in range(delta.days + 1):
            day = start_date + timedelta(days=i)
            file_op = day.strftime('%Y%m%d')
            
            # Reading bitcoin_output parquets
            file = ('blockchair_bitcoin_outputs_'+str(file_op)+'')
            realised_cap_usd = conn.execute("SELECT sum(value_usd)/100000000 AS realised_cap_usd FROM " + file).fetchone()
            realised_cap_btc = conn.execute("SELECT sum(value) AS realised_cap_btc FROM " + file).fetchone()

            # Inserts the newly computed realised cap into duckdb
            conn.execute('INSERT INTO realised_cap VALUES (?, ?, ?)', [day, realised_cap_usd[0], realised_cap_btc[0]])
    else:    
        # Creates timedelta
        delta = end_date - start_date   
        # Creates a realised_cap table if this is the first time the code is being run - instantiates the table
        conn.execute("""CREATE TABLE IF NOT EXISTS realised_cap (Date VARCHAR, "Realised Cap USD" DOUBLE, "Realised Cap BTC" DOUBLE)""")
        
        # Iterates through the timedelta
        for i in range(delta.days + 1):
            day = start_date + timedelta(days=i)
            file_op = day.strftime('%Y%m%d')
            
            # Reading bitcoin_output parquets
            file = ('blockchair_bitcoin_outputs_'+str(file_op)+'.parquet')
            realised_cap_usd = conn.execute("SELECT sum(value_usd)/100000000 AS realised_cap_usd FROM " + file).fetchone()
            realised_cap_btc = conn.execute("SELECT sum(value) AS realised_cap_btc FROM " + file).fetchone()

            # Inserts the newly computed realised cap into duckdb
            conn.execute('INSERT INTO realised_cap VALUES (?, ?, ?)', [day, realised_cap_usd[0], realised_cap_btc[0]])
        