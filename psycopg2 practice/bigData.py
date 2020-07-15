import psycopg2
import pandas as pd 
import os

'''
Here I will practice importing a CSV file in chunks instead all at once to avoid MemoryError when working with truly big files. 
The logic follows this process:

    1. Partition large csv into smaller CSV's
    2. Parse data for the first chunk
    3. Insert chunk to PostgreSQL table 
    4. Continue on to the second chunk
    ...
'''
        
lst = []

def partition(file_path='../resources/MOCK_DATA.csv' , chunk_size = 500):    
    '''
    parameter: chunk_size
    breaks down csv file into smaller csv files by chunk size
    '''
    batch_num = 1
    csvFile = pd.read_csv(file_path, chunksize=chunk_size)

    for chunk in csvFile:
        newFile = os.path.splitext('../resources/MOCK_DATA.csv')[0]
        chunk.to_csv(newFile + str(batch_num) + '.csv', index=False)
        batch_num+=1


def parse(chunk):
    df['money'] = df['money'].apply(lambda x: x.replace('$',''))
    df['date_of_birth'] = df['date_of_birth'].apply(lambda x: x.replace('/','-'))
    #return dfParsed
    pass

def writeSQL(chunk):
    pass 
    # insert, commit, close cursor



df = pd.read_csv('../resources/MOCK_DATA.csv', encoding='latin')

if __name__ == "__main__":
    partition()