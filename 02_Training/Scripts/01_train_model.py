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

#%pylab inline
#pylab.rcParams["figure.figsize"] = (10, 5)

INPUT = '../Data/prepared_train.h5'

# Read merged data for sampling
data = pd.read_hdf(INPUT, key='prepared_train')

date_cols = ['CAL_DATE_0', 'CAL_DATE_end_0', 'CAL_DATE_1', 'CAL_DATE_end_1', 'CAL_DATE_2', 'CAL_DATE_end_2', 'CAL_DATE_3',
 'CAL_DATE_end_3', 'CAL_DATE_4', 'CAL_DATE_end_4', 'CAL_DATE_5', 'CAL_DATE_end_5']

X = data.drop(['QUOTA_SELLOUT_5', 'BRANDFAMILY_ID'] + date_cols, axis=1)
y = data['QUOTA_SELLOUT_5']

# Split train and val datasets
customers = np.array(X['CUSTOMER_ID'].drop_duplicates())

index = np.random.permutation(len(customers))

ratio = 0.75
train = customers[:int(np.round(len(customers)*ratio))]
val = customers[int(np.round(len(customers)*ratio)):]

index_train = X['CUSTOMER_ID'].isin(train)
index_val = X['CUSTOMER_ID'].isin(val)

X_train = X[index_train].drop('CUSTOMER_ID', axis=1)
X_val = X[index_val].drop('CUSTOMER_ID', axis=1)

y_train = y[index_train]
y_val = y[index_val]

# Standard scaler
sc_X = StandardScaler()
X_train = sc_X.fit_transform(X_train)
X_val = sc_X.transform(X_val)

joblib.dump(sc_X, 'sc_X.bin', compress=True)

# Cube root trasform 
y_train = y_train**(float(1)/3)
y_val = y_val**(float(1)/3)

z = np.abs(stats.zscore(X_train))
X_train = X_train[(z < 3).all(axis=1)]
y_train = y_train[(z < 3).all(axis=1)]

"""
model = LinearRegression()

model = ltb.LGBMRegressor()

model = SVR()

model = BaggingRegressor(n_estimators=50, bootstrap_features=True, n_jobs=4, random_state=0, verbose=1)

model = KNeighborsRegressor(n_neighbors=3, n_jobs=4)

model = RandomForestRegressor(n_estimators = 100, n_jobs=4, random_state=0, verbose=1)

model= xgb.XGBRegressor(objective ='reg:squarederror', colsample_bytree = 0.3, learning_rate = 0.1,
                max_depth = 4, alpha = 10, n_estimators = 200, verbosity=1)


model.fit(X_train, y_train)

estimator = ltb.LGBMRegressor(verbose=1)

param_grid = {
    'learning_rate': [0.01, 0.1, 1],
    'n_estimators': [50, 100, 150] 
}

model = GridSearchCV(estimator, param_grid, cv=5)
model.fit(X_train, y_train)
"""

model = ltb.LGBMRegressor()
model.fit(X_train, y_train)


res_train = eval_model(model, X_train, y_train, [5, 10, 15])
res_val = eval_model(model, X_val, y_val, [5, 10, 15])

# save model
joblib.dump(model, 'model.pkl')


# val samples distribution
plt.hist(res_val['real'], range=[0,30], label='real', bins=20)
plt.legend(loc = 'real')
plt.hist(res_val['pred'], range=[0,30], label='pred', bins=20)
plt.legend(loc = 'pred')

# Plot the highest percentage error 
plt.hist(res_val.sort_values(by='percentage_error', ascending=False)['real'].iloc[:100])
# Plot the highest percentage error 
plt.hist(res_val.sort_values(by='absolute_error', ascending=False)['real'].iloc[:100])

# Plot results
res_sorted = res_val.sort_values(by='absolute_error', ascending=False).iloc[:100]
plt.plot(np.arange(len(res_sorted)), res_sorted['real'], label='real')
plt.legend(loc = 'real')
plt.plot(np.arange(len(res_sorted)), res_sorted['pred'], label='pred')
plt.legend(loc = 'pred')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))