# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import time

t1 = time.time()

INPUT_DATA_ORIGINAL = '../../04_Prediction/Data/quota_sellout_data_original.h5'
INPUT_DATA_PREDICTED = '../../04_Prediction/Data/qouta_sellout_data_predicted.h5'
OUTPUT = '../Data/qouta_sellout_data_combined.h5'

columns = ['CUSTOMER_ID', 'BRANDFAMILY_ID', 'Label', 'QUOTA_SELLOUT_5',
       'CAL_DATE_5', 'CAL_DATE_end_5', 'ABP_5', 'AZAFATA_5', 'CUE_5',
       'CLIPPER_5', 'DISPENSADOR_5', 'MECHERO_5', 'SVM_5', 'TFT_5', 'TOTEM_5',
       'VISIBILIDAD_5', 'VISIBILIDAD_ESP_5', 'VISIT_5']

#a = pd.read_hdf(INPUT_DATA_ORIGINAL, key='quota_sellout_data_original')[columns]
#b = pd.read_hdf(INPUT_DATA_PREDICTED, key='quota_sellout_data_predicted')[columns]

data = pd.concat([pd.read_hdf(INPUT_DATA_ORIGINAL, key='quota_sellout_data_original')[columns], pd.read_hdf(INPUT_DATA_PREDICTED, key='qouta_sellout_data_predicted')[columns]], 
                  ignore_index=True, sort=False)

data.to_hdf(OUTPUT, key='quota_sellout_data_combined', index=False, mode='w')

t2 = time.time()

print("Time:", str(t2-t1))



