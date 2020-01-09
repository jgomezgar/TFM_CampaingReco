# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 19:48:31 2019

@author: ESMICILO
"""

import os
import sys


init = str(sys.argv[1])
module = int(sys.argv[2])

init = 'N'
module = -1

if module == 0 or module == -1:
    if init.lower() in ['s', 'y', 'si', 'yes']: 
        if os.path.isfile('00_Extraction/Data/sellout_data.csv'):
            os.remove('00_Extraction/Data/sellout_data.csv')
            
        if os.path.isfile('00_Extraction/Data/no_sellout_data.csv'):
            os.remove('00_Extraction/Data/no_sellout_data.csv')
            
        if os.path.isfile('00_Extraction/Data/customers_ok15.csv'):
            os.remove('00_Extraction/Data/customers_ok15.csv')

if module == 1 or module == -1:
    if os.path.isfile('01_Preparation/Data/merged_sources.h5'):
        os.remove('01_Preparation/Data/merged_sources.h5')
    
    if os.path.isfile('01_Preparation/Data/sampled_train.h5'):
        os.remove('01_Preparation/Data/sampled_train.h5')
    
    if os.path.isfile('01_Preparation/Data/sampled_test.h5'):
        os.remove('01_Preparation/Data/sampled_test.h5')
    
    if os.path.isfile('01_Preparation/Data/cleaned_train.h5'):
        os.remove('01_Preparation/Data/cleaned_train.h5')
    
    if os.path.isfile('01_Preparation/Data/featured_train.h5'):
        os.remove('01_Preparation/Data/featured_train.h5')

if module == 2 or module == -1:
    if os.path.isfile('02_Training/Data/selected_features_train.h5'):
        os.remove('02_Training/Data/selected_features_train.h5')
        
    if os.path.isfile('02_Training/Data/prepared_train.h5'):
        os.remove('02_Training/Data/prepared_train.h5')

if module == 3 or module == -1: 
    if os.path.isfile('03_Evaluation/Data/prepared_test.h5'):
        os.remove('03_Evaluation/Data/prepared_test.h5')
    
    if os.path.isfile('03_Evaluation/Data/results_test.h5'):
        os.remove('03_Evaluation/Data/results_test.h5')

if module == 4 or module == -1:
    if os.path.isfile('04_Prediction/Data/prepared_no_sellout_data.h5'):
        os.remove('04_Prediction/Data/prepared_no_sellout_data.h5')
        
    if os.path.isfile('04_Prediction/Data/quota_sellout_data_original.h5'):
        os.remove('04_Prediction/Data/quota_sellout_data_original.h5')
        
    if os.path.isfile('04_Prediction/Data/qouta_sellout_data_predicted.h5'):
        os.remove('04_Prediction/Data/qouta_sellout_data_predicted.h5')
        
    if os.path.isfile('04_Prediction/Data/qouta_sellout_data_combined.h5'):
        os.remove('04_Prediction/Data/qouta_sellout_data_combined.h5')
        
    if os.path.isfile('04_Prediction/Data/quota_sellout_data_pivoted.h5'):
        os.remove('04_Prediction/Data/quota_sellout_data_pivoted.h5')
        
    if os.path.isfile('04_Prediction/Data/quota_sellout_norm.h5'):
        os.remove('04_Prediction/Data/quota_sellout_norm.h5')
