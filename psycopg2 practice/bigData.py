import psycopg2
import pandas as pd 

'''
Here I will practice importing a CSV file in chunks instead all at once to avoid MemoryError when working with truly big files. 
The logic follows this process:

    1. Import chunk
    2. Parse data
    3. Insert to PostgreSQL table 
    4. Continue on to the next chunk
'''
        
lst = []

def partition(chunksize = 10**2):    
    csvFile = pd.read_csv('../resources/MOCK_DATA.csv', chunksize=chunksize)
    for chunk in csvFile:
        df = chunk
        lst.append(df)
        return lst


def parse(chunk):
    df['money'] = df['money'].apply(lambda x: x.replace('$',''))
    df['date_of_birth'] = df['date_of_birth'].apply(lambda x: x.replace('/','-'))
    #return dfParsed

def writeSQL(chunk):
    pass 
    # insert, commit, close cursor



df = pd.read_csv('../resources/MOCK_DATA.csv', encoding='latin')

if __name__ == "__main__":
    partition()