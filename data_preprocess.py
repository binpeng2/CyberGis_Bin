import numpy as np
import pandas as pd
from collections import defaultdict
import datetime
import sys

def createat2date(str):
    """Convert a datetime in string format to date

    Args:
        str: the datetime to convert

    Returns:
        The date.

    """

    return str.split(' ')[0]

def data2chunks(read_path, write_folder):
    """

    Read the raw twitter streaming dataset, delete unnecessary columns.
    Then drop the rows in which author did not move from one place to
    another, and finally write chunks to disk.

    Args:
        read_path: the path of csv data
        write_folder: the destination folder.

    Returns:
        Nothing

    """

    chunksize = 10 ** 6
    for i, chunk in enumerate(pd.read_csv(read_path, chunksize=chunksize)):
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

def chunks2dict(read_folder, write_path, start=0, end=330):
    """

    Read chunks from a fixed path. Then process index from start
    to end. The number of visitors will be accumulated in the nested
    map, which will finally be converted to a df and written down.

    Args:
        read_folder: from this folder we read chunks
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
        df = pd.read_csv(read_folder + "/chunk_"+str(i)+".csv")
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

def accu_dict(read_path, write_path, time_length):
    """

    This function accumulate any days visitors in each cell. For example,
    if the recent past 7 days number of visitors is [1,2,3,4,5,6,7], the cell
    will be 28.

    Args:
        read_path: the path from which we read our data
        write_path: the path we write our data to (notice, we will add time_length as suffix)
        time_length: The time length we want to accumulate

    Returns:
        Nothing
        A new dataframe whose each cell contains the number of past time_length
        days of visitors.

    """

    df = pd.read_csv(read_path)
    for i in range(0, 1149):
        visitors_num = []
        for j in range(7, 126):
            visitors_num.append(int(df.iloc[:, j].at[i]))
            if len(visitors_num) > time_length:
                visitors_num.pop(0)
            time_accu = sum(visitors_num)
            # print((df.iloc[:, j].at[i], seven_accu, fourteen_accu))
            df.iloc[:, j].at[i] = time_accu
    df.to_csv(write_path + str(time_length) + ".csv")

def calc_percent(read_path, write_path):
    """

    This function calculate the percent change of number of visotors in
    time seires, and output a new dataframe which is the percent change
    of the original df.

    Args:
        read_path: The path of the file we are reading
        write_path: The path we save the output to

    Returns:
        Nothing
        A new dataframe whose each cell contains percent change of the
        number of past time_length days of visitors.

    """

    df = pd.read_csv(read_path)
    for i in range(0, 1149):
        prev_num = 0
        for j in range(7, 126):
            divider = (df.iloc[:, j].at[i] + prev_num) / 2.0
            print(prev_num, df.iloc[:, j].at[i], divider, round((df.iloc[:, j].at[i] - prev_num) / float(divider), 2))
            temp = df.iloc[:, j].at[i]
            print(type(temp))
            if divider != 0:
                df.iloc[:, j].at[i] = round((df.iloc[:, j].at[i] - prev_num) / float(divider), 3) * 100
            elif df.iloc[:, j].at[i] - prev_num != 0:
                df.iloc[:, j].at[i] = 999
            else:
                df.iloc[:, j].at[i] = 100
            if j == 7:
                df.iloc[:, j].at[i] = 100
            prev_num = temp
    df.to_csv(write_path)

if __name__ == "__main__":
    # data2chunks("./twitter.streaming6_byHuman_20200214_20200612/twitter.streaming6_byHuman_20200214_20200612.csv", './chunks_2/')
    # chunks2dict("./dicts_2/dict_0-329"+".csv", 0, 330)
    # accu_dict(14)
    # calc_percent("./dicts_2/accu_dict_0-329_7"+".csv", "./dicts_2/accu_dict_0-329_7_percent"+".csv")
    function_name = str(sys.argv[1])
    if function_name == "all":
        # To use this command, please make sure that you have already created the two folders
        # ./chunks_folder and ./dicts_folder
        data2chunks("./twitter.streaming6_byHuman_20200214_20200612/twitter.streaming6_byHuman_20200214_20200612.csv", './chunks_folder/')
        chunks2dict("./chunks_folder/", "./dicts_folder/dict_0-329.csv")
        accu_dict(14)
        calc_percent("./dicts_folder/accu_dict_0-329_14.csv", "./dicts_folder/accu_dict_0-329_14_percent.csv")
    elif function_name == "data2chunks":
        read_path = str(sys.argv[2])
        write_folder = str(sys.argv[3])
        data2chunks(read_path, write_folder)
    elif function_name == "chunks2dict":
        read_folder = str(sys.argv[2])
        write_path = str(sys.argv[3])
        chunks2dict(read_folder, write_path)
    elif function_name == "accu_dict":
        read_path = str(sys.argv[2])
        write_path = str(sys.argv[3])
        time_length = int(sys.argv[4])
        accu_dict(read_path, write_path, time_length)
    elif function_name == "calc_percent":
        read_path = str(sys.argv[2])
        write_path = str(sys.argv[3])
        calc_percent(read_path, write_path)
    else:
        print("Please enter correct function name")
        exit(1)