from datetime import timedelta
import pandas as pd


def get_realised_cap(start_date, end_date, conn, config):
    '''Creates the realised_cap table for Bitcoin in duckdb if it does not exist and inserts Bitcoin realised cap values into the table.'''
    # Creates timedelta
    delta = end_date - start_date

    # url strings
    str1 = "https://api.blockchair.com/bitcoin/outputs?a=date,sum(value),sum(value_usd)&q=is_spent(false),or,spending_time("
    str2 = "...),time(.."
    str3 = ")&export=csv&key=" + config['APIkey']['bitcoin']['blockchair']

    # Creates a realised_cap table if this is the first time the code is being run - instantiates the table
    conn.execute(
        """CREATE TABLE IF NOT EXISTS realised_cap (Date VARCHAR, "Realised Cap USD" DOUBLE, "Realised Cap BTC" DOUBLE)""")

    # Iterates through the timedelta
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        url = str1 + str(day) + str2 + str(day) + str3
        # Calls the API built by the url strings
        df = pd.read_csv(url)
        df.rename(columns={'sum(value)': 'value_btc',
                  'sum(value_usd)': 'value_usd'}, inplace=True)

        realised_cap_usd = df['value_usd'].sum()
        realised_cap_btc = df['value_btc'].sum() / 100000000
        realised_cap_df = pd.DataFrame({'Date': [day], 'Realised_Cap_USD': [
                                       realised_cap_usd], 'Realised_Cap_BTC_': [realised_cap_btc]})
        # Inserts the newly computed realised cap into duckdb
        conn.execute(
            """INSERT INTO realised_cap SELECT * FROM realised_cap_df""")
