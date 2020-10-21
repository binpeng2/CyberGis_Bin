import numpy as np
import pandas as pd
from collections import defaultdict

def createat2date(str):
    return str.split(' ')[0]

def data2chunks(data_path, write_folder):
    chunksize = 10 ** 6
    for i, chunk in enumerate(pd.read_csv(data_path, chunksize=chunksize)):
        print('Chunk: ', i)
        # print(chunk.head())
        # print(chunk.columns)
        df = chunk.loc[:, ['name', 'author_Id', 'created_at', 'location', 'place_type', 'place_full_name', 'place_country']]
        # print(df.head())
        drop_idxs = []
        last_Id_place_date = ('','')
        for index, row in df.iterrows():
            if row['place_type'] != 'city':
                drop_idxs.append(index)
                continue
            row['created_at'] = createat2date(row['created_at'])
            # print((row['author_Id'], row['place_full_name']), last_Id_place_date)
            if (row['author_Id'], row['place_full_name']) == last_Id_place_date:
                # skip repeat data piece
                drop_idxs.append(index)
            last_Id_place_date = (row['author_Id'], row['place_full_name'])
        df = df.drop(drop_idxs)
        df.to_csv(write_folder + 'chunk_' + str(i) + '.csv', index=False)

def chunks2dict(data_path, write_path):
    df = pd.read_csv(data_path)
    m = defaultdict(lambda : defaultdict(int))
    last_id = ''
    for index, row in df.iterrows():
        if row['author_id'] == last_id:
            m[row['place_full_name']][row['create_at']] += 1
    df = pd.DataFrame.from_dict(m, orient='index')
    df.to_csv(write_path)

if __name__ == "__main__":
    data2chunks("./twitter.streaming6_byHuman_20200214_20200612/twitter.streaming6_byHuman_20200214_20200612.csv", './chunks/')
