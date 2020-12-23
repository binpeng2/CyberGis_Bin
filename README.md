# CyberGis_Bin

This repo is Bin's work on CyberGIS Social Media project, including scripts to preprocess data and train time series data.

# Introduction

There are two files now: data_preprocess.py and risk_evaluate.py. And the followings are instuctions of using them.

data_preprocess.py:
There are two example callings in the main function. Before running it, please create two empty folder './dict' and './chunks'. 
Chunks will be generated after data2chunks call and visitors data will be generated after chunks2dict call. The result will 
be saved in the './dict' folder.

risk_evaluate.py:
This script contains functions to read data of any city, and functions to convert data to the form required by LSTM. Comments
will be completed later after successful training.

# User Guide for data preprocessing

0, There are four functions in the script:  
**data2chunks**: convert twitter stream data into chunks and write them to the folder    \
**chunks2dict**: read the chunks and generate dataframe for number of visitors to each city at each date   
**accu_dict**: read the dataframe, and accumulate the number of visitors to cities in past n days  
**calc_percent**: read the dataframe, calculate the percentage change at each date, and write a new file which replace number of visitors with percent change  
  
1, Create two folder "/chunks_folder" and "/dicts_folder"
  
2, If you have never run the scripts before, you could try run  
```
python data_preprocess.py all
```
This command will automatically run all the functions in order and generate datachunks, dataframe of number of visitors, dataframe of number of visitors in past 14 days, and dataframe of percent change of visitors in past 14 days  
  
3, If you want to use one function, just run
```
python data_preprocess.py function_name func_arg1 func_arg2
```
For example, if you want to use data2chunks:
```
python data_preprocess.py data2chunks ./twitter.streaming6_byHuman_20200214_20200612/twitter.streaming6_byHuman_20200214_20200612.csv ./chunks_folder/
```
  
4, More details about the functions, their arguments, and how to use them can be found in comments of the code. The main function also provides an example of how to use them. 

5, New command "python data_preprocess.py convert" could convert percentage change into js file. Make sure the inputCSV is valid and then 

# Progress:
1, I have written a script to extract number of visitors to any city from twitter_streaming data from twitter API.  
2, Collected COVID cases data of some MSA.  
3, Training model to evaluate COVID risk with visitors data with LSTM (tensorflow)  
4, Learned common algorithm for time-series prediction like LSTM and RNN.   
5, Add geoID infos, and accumulate 7, 14 days visitors now. New dataset can be found in folder dicts_2.  

# Planning:
1, Have a rubric model done, or have a rural formula for risk evaluation  
2, Learn the website and try to make my progress work on the website.
 
