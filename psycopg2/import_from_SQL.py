import pandas as pd 
import psycopg2 
import pprint



def to_pd():
    '''
    reads in a SQL query
    '''

    conn = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='postgres',
        host='localhost'
    )


    cursor = conn.cursor()

    query = "SELECT * FROM MOVIES_DETAILS;"

    data = pd.read_sql_query(sql=query, con=conn)

    cursor.close()
    conn.close()

    return data

def big_to_pd():
    '''
    reads in a large SQL query
    '''

    