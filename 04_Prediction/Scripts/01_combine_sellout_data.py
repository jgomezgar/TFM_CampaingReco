# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.externals.joblib import dump, load
import matplotlib.pyplot as plt 
from sklearn.externals import joblib
import time

t1 = time.time()


"""
%pylab inline
pylab.rcParams["figure.figsize"] = (10, 5)
"""

INPUT_TRAIN = '../../01_Preparation/Data/sampled_train.h5'
INPUT_TEST = '../../01_Preparation/Data/sampled_test.h5'
OUTPUT = '../Data/quota_sellout_data_original.h5'

"""
columns = ['CUSTOMER_ID', 'BRANDFAMILY_ID', 
           'CAL_DATE_5', 'CAL_DATE_1', 'CAL_DATE_2', 'CAL_DATE_3', 'CAL_DATE_4', 'CAL_DATE_5', 
           'CAL_DATE_end_5', 'CAL_DATE_end_1', 'CAL_DATE_end_2', 'CAL_DATE_end_3', 'CAL_DATE_end_4', 'CAL_DATE_end_5', 
           'ABP', 'AZAFATA', 'SVM', 'TFT', 'VISIBILIDAD_PROMO', 'VISITS_P', 'VISITS_S', 'VISITS_Z'
           'CUE_DESPLAZAMIENTO', 'CUE_SIN_DESPLAZAMIENTO',

 ]

base_cols = ['CUSTOMER_ID', 'BRANDFAMILY_ID', 'label', 'QUOTA_SELLOUT_5',]
columns = ['CAL_DATE', 'CAL_DATE_end', 'ABP', 'AZAFATA', 'CUE', 'CLIPPER', 'DISPENSADOR', 'MECHERO', 'SVM', 'TFT', 'TOTEM', 'VISIBILIDAD', 'VISIT']

cols = []
for element in columns:
    for i in range(0, -1, -1):
        cols.append(element+'_'+str(i))

total_columns = base_cols + cols


cols = []
for element in columns:
    for i in range(3, -1, -1):
        cols.append(element+'_'+str(i))
        
#a = pd.read_hdf(INPUT_TRAIN, key='sampled_train')

#b = pd.read_hdf(INPUT_TEST, key='sampled_test')
"""


data = pd.concat([pd.read_hdf(INPUT_TRAIN, key='sampled_train'), pd.read_hdf(INPUT_TEST, key='sampled_test')], 
                  ignore_index=True, sort=False)

data.to_hdf(OUTPUT, key='quota_sellout_data_original', index=False, mode='w')

t2 = time.time()

print("Time:", str(t2-t1))



