import numpy as np
import pandas as pd
from collections import defaultdict
import datetime

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
        df = chunk.loc[:, ['currentGEOID', 'lon', 'lat', 'name', 'author_Id', 'created_at', 'location', 'place_type', 'place_full_name', 'place_country']]
        # print(df.head())
        # break
        drop_idxs = []
        last_Id_place_date = ('', '')
        for index, row in df.iterrows():
            if row['place_type'] not in ['city', 'poi', 'neighborhood']:
                drop_idxs.append(index)
                continue
            row['created_at'] = createat2date(row['created_at'])
            if (row['author_Id'], row['currentGEOID']) == last_Id_place_date:
                drop_idxs.append(index)
            last_Id_place_date = (row['author_Id'], row['currentGEOID'])
        df = df.drop(drop_idxs)
        # print(df.head())
        df.to_csv(write_folder + 'chunk_' + str(i) + '.csv', index=False)
        # break

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
        df = pd.read_csv("./chunks_2/chunk_"+str(i)+".csv")
        for index, row in df.iterrows():
            if row['author_Id'] == last_id:
                place = row['currentGEOID']
                time = createat2date(row["created_at"])
                m[place][time] += 1
            last_id = row['author_Id']
    df = pd.DataFrame.from_dict(m, orient='index').sort_index(axis=1)
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'GEOID'})
    df = df.fillna(0)
    df = pd.DataFrame(df, dtype='int64')
    df.insert(1, "geoName", "")
    df.insert(2, "geoRegion", "")
    df.insert(3, "geoLON", "")
    df.insert(4, "geoLAT", "")
    df.insert(5, "ISO3", "")
    df_query = pd.read_csv("./GeoID_name.csv", encoding="ISO-8859-1")
    for index, row in df.iterrows():
        # print("The row geoid is: ", int(row["GEOID"]))
        geoInfo = df_query[df_query["GEOID"] == int(row["GEOID"])]
        # print(geoInfo)
        df.at[index, "geoName"] = geoInfo["NAME"].values[0] if len(geoInfo["NAME"].values) > 0 else "NaN"
        df.at[index, "geoRegion"] = geoInfo["region"].values[0] if len(geoInfo["region"].values) > 0 else "NaN"
        df.at[index, "geoLON"] = geoInfo["LON"].values[0] if len(geoInfo["LON"].values) > 0 else "NaN"
        df.at[index, "geoLAT"] = geoInfo["LAT"].values[0] if len(geoInfo["LAT"].values) > 0 else "NaN"
        df.at[index, "ISO3"] = geoInfo["ISO3"].values[0] if len(geoInfo["ISO3"].values) > 0 else "NaN"
    df.to_csv(write_path)

def accu_dict(time_length):
    """

    This function accumulate 7 and 14 days visitors in each cell

    """
    df = pd.read_csv("./dicts_2/dict_0-329.csv")
    for i in range(0, 1149):
        visitors_num = []
        for j in range(7, 126):
            visitors_num.append(int(df.iloc[:, j].at[i]))
            if len(visitors_num) > time_length:
                visitors_num.pop(0)
            time_accu = sum(visitors_num)
            # print((df.iloc[:, j].at[i], seven_accu, fourteen_accu))
            df.iloc[:, j].at[i] = time_accu
    df.to_csv("./dicts_2/accu_dict_0-329_" + str(time_length) + ".csv")

def calc_percent(read_path, write_path):
    df = pd.read_csv(read_path)
    for i in range(0, 1149):
        prev_num = 0
        for j in range(7, 126):
            divider = (df.iloc[:, j].at[i] + prev_num) / 2.0
            print(prev_num, df.iloc[:, j].at[i], divider, round((df.iloc[:, j].at[i] - prev_num) / float(divider), 2))
            temp = df.iloc[:, j].at[i]
            df.iloc[:, j].at[i] = round((df.iloc[:, j].at[i] - prev_num) / float(divider), 3) * 100 if divider != 0 else 0
            if j == 7:
                df.iloc[:, j].at[i] = 1
            prev_num = temp
    df.to_csv(write_path)

if __name__ == "__main__":
    # chunksize = 10 ** 6
    # data2chunks("./twitter.streaming6_byHuman_20200214_20200612/twitter.streaming6_byHuman_20200214_20200612.csv", './chunks_2/')
    # chunks2dict("./dicts_2/dict_0-329"+".csv", 0, 330)
    # accu_dict(14)
    calc_percent("./dicts_2/dict_0-329"+".csv", "./dicts_2/dict_0-329_percent"+".csv")
    # df = pd.read_csv("./dicts_2/accu_dict_0-329.csv")
    # print(df.head)
    # print(df.iloc[1]['2/14/2020'])
    # res = tuple(map(int, df.iloc[1]['2/14/2020'][1:-1].split(', ')))
    # print(res)
    # print(res[0])