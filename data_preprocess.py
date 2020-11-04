import numpy as np
import pandas as pd
from collections import defaultdict

def createat2date(str):
    """Convert a datetime in string format to date

    Args:
        str: the datetime to convert

    Returns:
        The date.

    """

    return str.split(' ')[0]

def data2chunks(data_path, write_folder):
    """

    Read the raw twitter streaming dataset, delete unnecessary columns.
    Then drop the rows in which author did not move from one place to
    another, and finally write chunks to disk.

    Args:
        data_path: the path of csv data
        write_folder: the destination folder.

    Returns:
        Nothing

    """

    chunksize = 10 ** 6
    for i, chunk in enumerate(pd.read_csv(data_path, chunksize=chunksize)):
        print('Chunk: ', i)
        df = chunk.loc[:, ['name', 'author_Id', 'created_at', 'location', 'place_type', 'place_full_name', 'place_country']]
        drop_idxs = []
        last_Id_place_date = ('','')
        for index, row in df.iterrows():
            if row['place_type'] != 'city':
                drop_idxs.append(index)
                continue
            row['created_at'] = createat2date(row['created_at'])
            if (row['author_Id'], row['place_full_name']) == last_Id_place_date:
                drop_idxs.append(index)
            last_Id_place_date = (row['author_Id'], row['place_full_name'])
        df = df.drop(drop_idxs)
        df.to_csv(write_folder + 'chunk_' + str(i) + '.csv', index=False)

def chunks2dict(write_path, start, end):
    """

    Read chunks from a fixed path. Then process index from start
    to end. The number of visitors will be accumulated in the nested
    map, which will finally be converted to a df and written down.

    Args:
        write_path: where to write the final result
        start: the index of start chunk
        end: the index of end chunk

    Returns:
        Nothing

    """
    m = defaultdict(lambda: defaultdict(int))
    last_id = ''
    for i in range(start, end):
        print("Processing: ", i)
        df = pd.read_csv("./chunks/chunk_"+str(i)+".csv")
        for index, row in df.iterrows():
            if row['author_Id'] == last_id:
                place = row['place_full_name']
                time = createat2date(row["created_at"])
                m[place][time] += 1
            last_id = row['author_Id']
    df = pd.DataFrame.from_dict(m, orient='index').sort_index(axis=1)
    df = df.fillna(0)
    df = pd.DataFrame(df, dtype='int64')
    df.to_csv(write_path)

# def merge_dict():
#     df = None
#     for i in range(n):
#         df_i = pd.read_csv('./dicts/dict_'+str(i)+'.csv')
#         if not df:
#             df = df_i
#         for row_idx, row in df_i.iterrows():
#             df[]


if __name__ == "__main__":
    # data2chunks("./twitter.streaming6_byHuman_20200214_20200612/twitter.streaming6_byHuman_20200214_20200612.csv", './chunks/')
    chunks2dict("./dicts/dict_0-329"+".csv", 0, 330)