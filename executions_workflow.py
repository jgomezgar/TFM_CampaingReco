# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 18:18:09 2019

@author: Jose Luis Gomez
"""

import os
import sys

init = str(sys.argv[1])
state = int(sys.argv[2])
ignore_eval = str(sys.argv[3])

#init = 'n'
#state = 2
#ignore_eval = 'N'

if init.lower() in ['s', 'y', 'si', 'yes']: 
    os.chdir("00_Extraction/Scripts/")
    
    print('Beginning extracting SQL data...')
    os.system('python 00_extraction.py')
    print('SQL data extracted!\n')

if state >= 1:
    if init.lower() in ['s', 'y', 'si', 'yes']:
        os.chdir("../../01_Preparation/Scripts/")
    else:
        os.chdir("01_Preparation/Scripts/")
    print('Beginning merging sources...')
    os.system('python 00_merge_sources.py')
    print('Data merged!\n')
    
    print('Beginning sampling train and test data...')
    os.system('python 01_sampling_train_test_dataset.py')
    print('Data sampled!\n')
    
    print('Beginning cleaning train data...')
    os.system('python 02_data_cleaning.py')
    print('Train data cleaned!\n')
    
    print('Beginning feature engineering...')
    os.system('python 03_feature_engineering.py')
    print('Added new train data features!\n')
    
if state >= 2:
    os.chdir("../../02_Training/Scripts/")
    print('Beginning selecting best features...')
    os.system('python 00_feature_selection.py')
    print('Best features selected for train data!\n')
    
    print('Beginning training model...')
    os.system('python 01_train_model.py')
    print('Model trained!\n')
    
if state >= 3 and ignore_eval.lower() != 's':
    os.chdir("../../03_Evaluation/Scripts/")
    print('Beginning preparing, cleaning and adding new features to test data...')
    os.system('python 00_prepare_data_test.py')
    print('Test data prepared for testing model!\n')
    
    print('Beginning evaluating the model with test data...')
    os.system('python 01_evaluate_model.py')
    print('Model evaluated!\n')

if state >= 4:
    os.chdir("../../04_Prediction/Scripts/")
    print('Beginning preparing, cleaning and adding new features to no sellout data...')
    os.system('python 00_prepare_no_sellout_data.py')
    print('No sellout data prepared!\n')
    
    print('Beginning combining train and test data with sellout...')
    os.system('python 01_combine_sellout_data.py')
    print('Sellout data combined!\n')
    
    print('Predicting customers without sellout...')
    os.system('python 02_predict_sellout.py')
    print('New sellout predicted variable added!\n')
    
    print('Beginning combining no sellout data with sellout data...')
    os.system('python 03_combine_quota_sellout.py')
    print('All data with and without sellout combined!\n')
    
    print('Pivoted data to get same variable three times in same row...')
    os.system('python 04_pivot_three_times_data.py')
    print('Data pivoted!\n')
    
    print('Getting differences between quota sellout and normalizing with seasonality...')
    os.system('python 05_normalize_quota_sellout.py')
    print('Data normalized!\n')