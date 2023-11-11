def load_to_pg(engine, dbconn, start_date, end_date, config):

    cryptocurrencies = config['cryptocurrencies']

    for crypto in cryptocurrencies:
        computed_metrics_df = config['pg_table_names']['computed_metrics'][crypto]
        computed_query = f"SELECT * FROM {computed_metrics_df} WHERE Date BETWEEN '" + \
            start_date.strftime('%Y-%m-%d') + "' AND '" + \
            end_date.strftime('%Y-%m-%d') + "'"
        computed_df = dbconn.execute(
            computed_query).fetchdf().sort_values(by=['Date'])

        basic_metrics_df = config['pg_table_names']['basic_metrics'][crypto]
        basic_query = f"SELECT * FROM {basic_metrics_df} WHERE Date BETWEEN '" + \
            start_date.strftime('%Y-%m-%d') + "' AND '" + \
            end_date.strftime('%Y-%m-%d') + "'"
        basic_df = dbconn.execute(
            basic_query).fetchdf().sort_values(by=['Date'])

        # print(basic_df.sort_values(by = ['Date']))

        computed_df.to_sql(f'{computed_metrics_df}', engine,
                           index=False, if_exists='replace')
        basic_df.to_sql(f'{basic_metrics_df}', engine,
                        index=False, if_exists='replace')
        # print(dbconn.execute("Select * from computed_metrics").fetchall())
