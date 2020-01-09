# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.externals.joblib import dump, load
from sklearn.linear_model import LinearRegression, SGDRegressor, Perceptron, PassiveAggressiveRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor, BaggingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVR, LinearSVR
import xgboost as xgb
import lightgbm as ltb
import matplotlib.pyplot as plt 
from sklearn.externals import joblib
from sklearn.preprocessing import PolynomialFeatures
import dask.dataframe as dd 
import dask
import time
from tqdm import tqdm
import sys

t1 = time.time()

def change_in_quota(row):
    past1 = row['QUOTA_SELLOUT_1']
    past2 = row['QUOTA_SELLOUT_2']
    past3 = row['QUOTA_SELLOUT_3']
    present = row['QUOTA_SELLOUT_0']
    if np.nan in [past3, past2, past1]:
        value = np.nan
    else: 
        mean_value = np.mean([past3, past2, past1])
        value = present-mean_value
    return value

def norm_month_quota(row):
    actual_month = row['actual_month']
    column='median_'+str(actual_month)
    if actual_month<10:
        column='median_0'+str(actual_month)
    
    value = row['change_in_quota']-row[column]
    return value


INPUT_PIVOT_DATA = '../Data/quota_sellout_data_pivoted.h5'
INPUT_NORM = '../Data/variaciones_cuotas_sellin_historicas.csv'
INPUT_BRANDS = '../Data/equivalencia_marcas.csv'
OUTPUT = '../Data/quota_sellout_norm.h5'

# Preprocessing test data

data = pd.read_hdf(INPUT_PIVOT_DATA, key='quota_sellout_data_pivoted')
norm = pd.read_csv(INPUT_NORM, sep='|')
brands = pd.read_csv(INPUT_BRANDS, sep=';')

##########################################################################################
# TEMPORAL FIX

brands['BRANDFAMILY'] = brands['BRANDFAMILY'].str.replace(' - ', ' ')
brands['BRANDFAMILY'] = brands['BRANDFAMILY'].str.replace(' ', '_')
brands['BRANDFAMILY'] = brands['BRANDFAMILY'].str.lower()

data = data.merge(brands[['BRANDFAMILY_ID', 'BRANDFAMILY']], on='BRANDFAMILY_ID', how='inner')

#tmp = data[['CUSTOMER_ID', 'BRANDFAMILY_ID']].reset_index()
#norm['brandfamily'] = tmp['BRANDFAMILY_ID'].loc[:len(norm)]
##########################################################################################

data = data.merge(norm, left_on=['CUSTOMER_ID', 'BRANDFAMILY'], right_on=['customer_id', 'brandfamily'], how='left')

data.dropna(how='any', inplace=True)

data['change_in_quota'] = data.apply(change_in_quota, axis=1)
data['change_in_quota_norm'] = data.apply(norm_month_quota, axis=1)

to_delete = ['customer_id', 'brandfamily', 'actual_month', 'QUOTA_SELLOUT_1', 'QUOTA_SELLOUT_2', 'QUOTA_SELLOUT_3']
for element in list(data.columns):
    if 'median' in element:
        to_delete.append(element)
    if 'CAL_DATE_end' in element:
        to_delete.append(element)    
    
data.drop(to_delete, axis=1, inplace=True)

data.to_hdf(OUTPUT, key='quota_sellout_norm', index=False, mode='w')

t2 = time.time()

print("Time:", str(t2-t1))

#month_quota_all_total = pd.read_csv('C:/Users/esmicilo/Documents/Altadis-Data-Science/Sprint 4/00_Analysis/02_data_unification/Data/month_quota_all_total.csv', sep='|', nrows=10000)
