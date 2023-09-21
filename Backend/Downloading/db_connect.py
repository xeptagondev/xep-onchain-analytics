from sqlalchemy import create_engine
import psycopg2
import json
import os
def db_connect():
    engine = create_engine(os.getenv('POST_ENGINE'))
    psqlconn = psycopg2.connect(database = os.getenv('POST_DATABASE'),
                                    host = os.getenv('POST_HOST'),
                                    user = os.getenv('POST_USER'),
                                    password = os.getenv('POST_PASSWORD'),
                                    port = os.getenv('POST_PORT'))

    
    return engine , psqlconn

