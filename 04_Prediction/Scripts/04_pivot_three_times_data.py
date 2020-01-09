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

"""
################################################
month_quota = pd.read_csv('../Data/month_quota.csv', sep='|', nrows=10000)  


INPUT = '../../01_Preparation/Data/sampled_train.h5'
INPUT = '../../04_Prediction/Data/prepared_no_sellout_data.h5'
sampled = pd.read_hdf(INPUT, key='prepared_no_sellout_data').iloc[:10000]  

for element in list(sampled.columns.values):
    if 'AZAFATA_1' in element:
        print("EXISTS")

cols = pd.Series(list(month_quota.columns.values)).str.upper()
cols = cols.str.replace('1', '')
cols = cols.str.replace('2', '')
cols = cols.str.replace('3', '')
cols = list(np.unique(cols))

actual_cols = pd.Series(list(data.columns.values)).str.lower()
actual_cols = actual_cols.str.replace('_0', '')
actual_cols = actual_cols.str.replace('_1', '')
actual_cols = actual_cols.str.replace('_2', '')
actual_cols = actual_cols.str.replace('_3', '')
actual_cols = actual_cols.str.replace('_4', '')
actual_cols = actual_cols.str.replace('_5', '')
actual_cols = actual_cols.str.replace('_6', '')
actual_cols = list(np.unique(actual_cols))

not_included = []
for col in cols:
    if col not in actual_cols:
        not_included.append(col)


included = []
for col in cols:
    if col in actual_cols:
        included.append(col)

included = list(pd.Series(included).str.upper())
################################################
"""

t1 = time.time()

INPUT_QUOTA_SELLOUT = '../Data/qouta_sellout_data_combined.h5'
INPUT_NORM = '../Data/variaciones_cuotas_sellin_historicas.csv'
OUTPUT = '../Data/quota_sellout_norm.h5'

# Preprocessing test data

#data = pd.read_hdf(INPUT_QUOTA_SELLOUT, key='qouta_sellout_data_combined').sample(30000)
data = pd.read_hdf(INPUT_QUOTA_SELLOUT, key='qouta_sellout_data_combined')

data['CAL_DATE_5'] = pd.to_datetime(data['CAL_DATE_5'], format='%Y-%m-%d', errors='coerce')
data['CAL_DATE_end_5'] = pd.to_datetime(data['CAL_DATE_5'], format='%Y-%m-%d', errors='coerce')

data.dropna(inplace=True)

data['actual_month_5'] = data['CAL_DATE_5'].map(lambda value: value.month)

customers_brands = data[['CUSTOMER_ID', 'BRANDFAMILY_ID']]
customers_brands = pd.DataFrame(customers_brands.groupby(['CUSTOMER_ID', 'BRANDFAMILY_ID'],as_index=False).size()).reset_index()
customers_brands.columns = ['CUSTOMER_ID', 'BRANDFAMILY_ID', "SIZE"]
customers_brands = customers_brands[customers_brands['SIZE'] > 3]

fields = list(data.columns)

loop_cols = fields.copy()

loop_cols.remove('CUSTOMER_ID')
loop_cols.remove('BRANDFAMILY_ID')
loop_cols.remove('Label')

total_columns = []
for i in range(0, 4):
    for f in list(loop_cols):
        total_columns.append(f[:len(f)-1]+str(i))
        

"""
def get_pivot_data(data, cb):
    c = cb[0]
    b = cb[1]

    cust_brand = data[(data["CUSTOMER_ID"] == c) & (data["BRANDFAMILY_ID"] == b)].sort_values('CAL_DATE_5', ascending=False)
    label = cust_brand['Label'].iloc[0]
    cust_brand.drop(['CUSTOMER_ID', 'BRANDFAMILY_ID', 'Label'], axis=1, inplace=True)
    
    pivot_data = []
    
    cnt=1
    
    for _, row in cust_brand.iterrows():
        print(_, row)
        if cnt > len(cust_brand)-3:
            break
    
        quota_row = np.array(row).reshape(1,-1)
        df = cust_brand.iloc[cnt:cnt+3].values.flatten().reshape(1,-1)
        
        concat_data = np.concatenate([quota_row, df], axis=1).reshape(-1)
        pivot_data.append(concat_data)
        
        cnt+=1
    
    pivot_data = pd.DataFrame(pivot_data, columns = total_columns)

    pivot_data['CUSTOMER_ID'] = c
    pivot_data['BRANDFAMILY_ID'] = b
    pivot_data['Label'] = label
    
    #pivot_data.to_csv('tmp_data/pivot_data_'+str(c)+b+'.csv', index=False, sep='|', mode='w')
    return pivot_data


import multiprocessing as mp
pool = mp.Pool(mp.cpu_count())

t1 = time.time()
res = pool.starmap(get_pivot_data, [(data, cb) for _, cb in tqdm(customers_brands.iterrows())])
t2 = time.time()

print ("Time:",t2-t1)
pool.close()

res = pool.map(get_pivot_data, [cb for _, cb in tqdm(customers_brands.iterrows())])

pool.map(get_pivot_data, [row for row in data])
 
pool.close()
"""

# duplicates row with same date, customer and brand
#c = 1000017
#b = 'BF131001'

# Bucle para obtener todas las tablas pivote de cliente y marca
t1 = time.time()

limit = 2000
counter = 0
all_data = pd.DataFrame(columns=total_columns)
for _, cb in tqdm(customers_brands.iterrows(), total=customers_brands.shape[0]):
    c = cb[0]
    b = cb[1]

    cust_brand = data[(data["CUSTOMER_ID"] == c) & (data["BRANDFAMILY_ID"] == b)].sort_values('CAL_DATE_5', ascending=False)
    label = cust_brand['Label'].iloc[0]
    cust_brand.drop(['CUSTOMER_ID', 'BRANDFAMILY_ID', 'Label'], axis=1, inplace=True)
    
    pivot_data = []
    
    cnt=1
    
    for _, row in cust_brand.iterrows():
        if cnt > len(cust_brand)-3:
            break
    
        quota_row = np.array(row).reshape(1,-1)
        df = cust_brand.iloc[cnt:cnt+3].values.flatten().reshape(1,-1)
        
        concat_data = np.concatenate([quota_row, df], axis=1).reshape(-1)
        pivot_data.append(concat_data)
        
        cnt+=1
    
    pivot_data = pd.DataFrame(pivot_data, columns = total_columns)

    pivot_data['CUSTOMER_ID'] = c
    pivot_data['BRANDFAMILY_ID'] = b
    pivot_data['Label'] = label
    
    if len(all_data) == 0:
        all_data = pivot_data.copy()
    else:
        all_data = pd.concat([all_data, pivot_data])
        
    #pivot_data.to_hdf('tmp_data/pivot_data_'+str(c)+b+'.h5', key='pivot_data_'+str(c)+b, index=False, mode='w')
    
    #pivot_data.to_hdf('../Data/all_pivot_data.h5', key='pivot_data', index=False, mode='a')
    
    counter+=1
    
    if counter > limit:
        break
        
all_data.drop(columns = ['actual_month_1', 'actual_month_2', 'actual_month_3'], axis=1, inplace=True)
all_data.rename(columns={'actual_month_0':'actual_month'}, inplace=True)

for element in list(all_data.columns):
    if 'CAL_DATE' in element[:-2]:
        num = element[-2:]
        all_data[element[:-2]+num] = pd.to_datetime(all_data[element[:-2]+num], format='%Y-%m-%d')

columns = pd.Series(all_data.columns).sort_values()

all_data = all_data[columns].sort_values(by=['CUSTOMER_ID', 'BRANDFAMILY_ID', 'CAL_DATE_0'])

all_data.to_hdf('../Data/quota_sellout_data_pivoted.h5', key='quota_sellout_data_pivoted', index=False, mode='w')

t2 = time.time()

print ("Time:",t2-t1)

