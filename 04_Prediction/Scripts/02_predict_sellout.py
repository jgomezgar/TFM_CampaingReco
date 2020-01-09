# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.externals.joblib import dump, load
import matplotlib.pyplot as plt 
from sklearn.externals import joblib
import dask.dataframe as dd 
import time

t1 = time.time()

"""
%pylab inline
pylab.rcParams["figure.figsize"] = (10, 5)
"""

INPUT_DATA = '../Data/prepared_no_sellout_data.h5'
INPUT_FEATS = '../../02_Training/Scripts/features.npy'
INPUT_MODEL = '../../02_Training/Scripts/model.pkl'
INPUT_SC = '../../02_Training/Scripts/sc_X.bin'
OUTPUT = '../Data/qouta_sellout_data_predicted.h5'

data = pd.read_hdf(INPUT_DATA, key='prepared_no_sellout_data')

features = list(np.load(INPUT_FEATS)) + ['BRANDFAMILY_ID']
features.remove('QUOTA_SELLOUT_5')
X = data[features]
       
date_cols = ['CAL_DATE_0', 'CAL_DATE_end_0', 'CAL_DATE_1', 'CAL_DATE_end_1', 'CAL_DATE_2', 'CAL_DATE_end_2', 'CAL_DATE_3',
 'CAL_DATE_end_3', 'CAL_DATE_4', 'CAL_DATE_end_4', 'CAL_DATE_5', 'CAL_DATE_end_5']
 
X = X.drop(['CUSTOMER_ID', 'BRANDFAMILY_ID'] + date_cols, axis=1)

# Standard scaler
sc_X = joblib.load(INPUT_SC)
X = sc_X.transform(X)

# Load trained model and evaluate test data
model = joblib.load(INPUT_MODEL)
y_pred = model.predict(X)
y_pred = np.power(y_pred,3)

data['QUOTA_SELLOUT_5'] = y_pred

data.to_hdf(OUTPUT, key='qouta_sellout_data_predicted', index=False, mode='w')

t2 = time.time()

print("Time:", str(t2-t1))



