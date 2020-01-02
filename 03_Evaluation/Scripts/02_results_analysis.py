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
import time 

t1 = time.time()

#%pylab inline
#pylab.rcParams["figure.figsize"] = (10, 5)

INPUT = '../Data/results_test.h5'

# Preprocessing test data
data = pd.read_hdf(INPUT, key='results_test')

umbral = 5
X = data[data['absolute_error'] > 5]

X_info = X.describe()

# Test samples distribution
plt.hist(X['real'], range=[X['real'].min(),X['real'].max()], label='real', bins=40)
plt.legend(loc = 'real')

# Plot the highest percentage error 
plt.hist(X.sort_values(by='percentage_error', ascending=False)['real'].iloc[:100])
# Plot the highest percentage error 
plt.hist(X.sort_values(by='absolute_error', ascending=False)['real'].iloc[:100])

# Plot results
res_sorted = X.sort_values(by='absolute_error', ascending=False).iloc[:100]
plt.plot(np.arange(len(res_sorted)), res_sorted['real'], label='real')
plt.legend(loc = 'real')
plt.plot(np.arange(len(res_sorted)), res_sorted['pred'], label='pred')
plt.legend(loc = 'pred')

t2 = time.time()

print("Time:", str(t2-t1))