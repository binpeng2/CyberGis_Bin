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

# User Guide



# Progress:
1, I have written a script to extract number of visitors to any city from twitter_streaming data from twitter API.  
2, Collected COVID cases data of some MSA.  
3, Training model to evaluate COVID risk with visitors data with LSTM (tensorflow)  
4, Learned common algorithm for time-series prediction like LSTM and RNN.   
5, Add geoID infos, and accumulate 7, 14 days visitors now. New dataset can be found in folder dicts_2.  

# Planning:
1, Have a rubric model done, or have a rural formula for risk evaluation  
2, Learn the website and try to make my progress work on the website.
 
