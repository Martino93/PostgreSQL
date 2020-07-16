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
                host='localhost')


    cursor = conn.cursor()

    query = "SELECT * FROM movies_details;"

    data = pd.read_sql_query(sql=query, con=conn)

    cursor.close()
    conn.close()

    return data



def big_to_pd(chunk_size=100, offset=0):
    '''
    reads in a large SQL query without running out of memory
    chunk_size: INT length of each df 
    offset: INT where to begin the next iteration 

    GOAL: write a pickle file from a massive database
    '''
    conn = psycopg2.connect(
                database='postgres',
                user='postgres',
                password='postgres',
                host='localhost')

    dfs = []
    
    while True:

        query = 'SELECT * FROM movies_details LIMIT {} OFFSET {}'.format(chunk_size, offset)

        df = pd.read_sql_query(sql=query, con=conn)
        print(f'started reading at {offset} in chunks of {chunk_size}')
        dfs.append(df)

        offset += chunk_size

        # here is where I would include any parsing/data munging


        # if still too much to handle in dfs list don't append and go straight to writing csv chunk. 




        # stops if the size of the last df in the list is less than 100 rows long  
        if len(dfs[-1]) < chunk_size:
            break

    full_df = pd.concat(dfs)

    # create pickle file 
    full_df.to_pickle('../pickles/test.pkl')



# read it in 
pd.read_pickle('../pickles/test.pkl')


# how do i check for duplicates in SQL table?