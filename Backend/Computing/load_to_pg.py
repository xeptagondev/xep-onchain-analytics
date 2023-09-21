def load_to_pg(engine, dbconn, start_date, end_date):

    computed_query = "SELECT * FROM computed_metrics WHERE Date BETWEEN '" + start_date.strftime('%Y-%m-%d') + "' AND '" + end_date.strftime('%Y-%m-%d') + "'"
    computed_df = dbconn.execute(computed_query).fetchdf().sort_values(by=['Date'])

    basic_query = "SELECT * FROM basic_metrics WHERE Date BETWEEN '" + start_date.strftime('%Y-%m-%d') + "' AND '" + end_date.strftime('%Y-%m-%d') + "'"
    basic_df = dbconn.execute(basic_query).fetchdf().sort_values(by=['Date'])

    # print(basic_df.sort_values(by = ['Date']))

    computed_df.to_sql('computed_metrics', engine, index = False, if_exists = 'append')
    basic_df.to_sql('basic_metrics', engine, index = False, if_exists = 'append')
    # print(dbconn.execute("Select * from computed_metrics").fetchall())
    