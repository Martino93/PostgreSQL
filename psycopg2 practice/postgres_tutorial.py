import pandas as pd 
import psycopg2
import numpy as np 
from datetime import datetime



conn = psycopg2.connect(
    host = 'localhost',
    database = 'postgres',
    user = 'postgres',
    password = 'postgres'
)


movies = pd.read_csv('http://bit.ly/imdbratings')


# replace apostrophies and ('u')
movies['title'] = movies['title'].str.replace(r"\'",'')
movies['actors_list'] = movies['actors_list'].apply(
    lambda x: x.replace("u", "").\
        replace("\'","").\
            replace("[", "").\
                replace("]", ""))

# adding a coumn of random dates to "movies"
movies["dates"] = np.random.choice(pd.date_range('1980-01-01','2020-01-01'), len(movies))

# converting datetime to string to pass it in INSERT command below
movies['dates'] = movies['dates'].apply(lambda x: datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))


# insert dataframe records one by one 
cursor = conn.cursor()
for i, _ in movies.iterrows(): 
    insert = 'INSERT INTO movies_details \
            (star_rating, title, content_rating, genre, duration, actors_list, date) \
            VALUES ({}, \'{}\', \'{}\',\'{}\',{}, \'{}\',TIMESTAMP \'{}\')'.\
                format(movies.loc[i,'star_rating'], 
                movies.loc[i,"title"], 
                movies.loc[i,"content_rating"],
                movies.loc[i,"genre"],
                movies.loc[i,"duration"],
                movies.loc[i,"actors_list"],
                movies.loc[i,'dates']
                ) 

    cursor.execute(insert)
    conn.commit()


cursor.close()

   

def drop_table(conn):
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE movies_details''')
    conn.commit()
    cursor.close()

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE movies_details(
        star_rating FLOAT,
        title VARCHAR(200),
        content_rating VARCHAR(10),
        genre VARCHAR(10),
        duration INT,
        actors_list VARCHAR(500),
        date DATE);
        ''')
    conn.commit()
    cursor.close()

def select(con= conn, limit= '10'):
    results = []
    cursor = conn.cursor()
    cursor.execute(f'select * from movies_details limit {limit}')
    rows = cursor.fetchall()
    cursor.close()

    for r in rows:
        results.append(r)
    
    return results


# practice reading in csv with millions of rows


# practice deleting duplicates
