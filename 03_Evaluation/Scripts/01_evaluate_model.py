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

t1 = time.time()

def eval_model(model, X, y, thresholds=[5]):
    y_pred = model.predict(X)
    y = np.power(y,3)
    y_pred = np.power(y_pred,3)
    porc_error = abs(y_pred - y)*100/y
    absolute_mean_error = mean_absolute_error(y, y_pred)
    porcentual_mean_error = np.mean(porc_error[porc_error != np.inf])
    
    print("\nTEST: Absolute Error:", absolute_mean_error)
    print("Porcentual Error:", porcentual_mean_error)
    print("STD Error:", np.std(abs(y - y_pred)))
    print("R2 Score:", r2_score(y, y_pred))
    results = pd.DataFrame(np.array(y), columns = ['real'])
    results['pred'] = y_pred
    results['percentage_error'] = np.abs(results['pred'] - results['real'])*100/results['real'] 
    results['absolute_error'] = np.abs(results['pred'] - results['real'])
    
    for threshold in thresholds:
        hits = 0
        for element in results['absolute_error']:
            if element <= threshold:
                hits+=1
        
        porcentual_hits = hits*100/len(results)
        print(str(porcentual_hits)+str("% registers are with less than"), str(threshold)+str("% of absolute error."))
        
    return results

"""
%pylab inline
pylab.rcParams["figure.figsize"] = (10, 5)
"""

INPUT_DATA = '../Data/prepared_test.h5'
INPUT_MODEL = '../../02_Training/Scripts/model.pkl'
INPUT_FEATS = '../../02_Training/Scripts/features.npy'
INPUT_SC = '../../02_Training/Scripts/sc_X.bin'
OUTPUT = '../Data/results_test.h5'

date_cols = ['CAL_DATE_0', 'CAL_DATE_end_0', 'CAL_DATE_1', 'CAL_DATE_end_1', 'CAL_DATE_2', 'CAL_DATE_end_2', 'CAL_DATE_3',
 'CAL_DATE_end_3', 'CAL_DATE_4', 'CAL_DATE_end_4', 'CAL_DATE_5', 'CAL_DATE_end_5']

# Preprocessing test data
X = pd.read_hdf(INPUT_DATA, key='prepared_test')

#features = np.load(INPUT_FEATS)
#X = X[features]

X_test = X.drop(['CUSTOMER_ID', 'BRANDFAMILY_ID', 'QUOTA_SELLOUT_5'] + date_cols, axis=1)
y_test = X['QUOTA_SELLOUT_5']

# Standard scaler
sc_X = joblib.load(INPUT_SC)
X_test = sc_X.transform(X_test)

# Cube root trasform 
y_test = y_test**(float(1)/3)

# Load trained model and evaluate test data
model = joblib.load(INPUT_MODEL)
res_test = eval_model(model, X_test, y_test, [5, 10, 15])


# Test samples distribution
plt.hist(res_test['real'], range=[0,30], label='real', bins=20)
plt.legend(loc = 'real')

# Plot the highest percentage error 
plt.hist(res_test.sort_values(by='percentage_error', ascending=False)['real'].iloc[:100])
# Plot the highest percentage error 
plt.hist(res_test.sort_values(by='absolute_error', ascending=False)['real'].iloc[:100])

# Plot results
res_sorted = res_test.sort_values(by='absolute_error', ascending=False).iloc[:100]
plt.plot(np.arange(len(res_sorted)), res_sorted['real'], label='real')
plt.legend(loc = 'real')
plt.plot(np.arange(len(res_sorted)), res_sorted['pred'], label='pred')
plt.legend(loc = 'pred')

data = pd.concat([X.reset_index(), res_test], axis=1)

data.to_hdf(OUTPUT, key='results_test', index=False, mode='w')

t2 = time.time()

print("Time:", str(t2-t1))



