
# Import the libraries
import pandas as pd
import numpy as np
import lightgbm as ltb
import seaborn as sns
from sklearn.preprocessing import StandardScaler
#import dask.dataframe as dd
import time 

t1 = time.time()

INPUT = '../../01_Preparation/Data/featured_train.h5'
OUTPUT_FEATS = 'features.npy'
OUTPUT = '../Data/prepared_train.h5'

# Read merged data for sampling
data = pd.read_hdf(INPUT, key='featured_train')

date_cols = []
for field in data.columns.values:
    element = field.lower()
    if 'fecha' in element or 'date' in element:
        date_cols.append(field)

selected = True

if not selected:

    cols = []
    for field in data.columns.values:
        element = field.lower()
        if ('so_' in element) or ('si' in element and '5' in element) \
        or ('sellout' in element and '_5' not in element) or ('sellin' in element and '5' in element) \
        or 'fecha' in element or 'date' in element:
            cols.append(field)
            
    to_delete = cols+['Midcategory', 'R_0', 'R_1', 'R_2', 'R_3', 'R_4', 'R_5']
    
    X = data.drop(to_delete, axis=1)
    
    
    """
    # GENERATE DUMMY TABLE FOR BRAND_FAMILY AND LABEL
    
    """
    X = X.drop(['BRANDFAMILY_ID', 'label', 'customer_id', 'CUSTOMER_ID', 'QUOTA_SELLOUT_5', 'OK_13M', 'OK_15M'], axis=1)
    y = data['QUOTA_SELLOUT_5']
    
    # Remove features more correlated between them
    correlations = X.corr()
    
 #   sns.heatmap(correlations)
    
    threshold = 0.95
    ignored_fields = []
    for c in correlations.iterrows():
        sort = c[1].sort_values(ascending=False)
        if sort[1] > threshold or sort[1] < -threshold:
            correlations = correlations.drop(c[0], axis = 0)
            correlations = correlations.drop(c[0], axis = 1)
            ignored_fields.append(c[0])
    
    X = X.drop(ignored_fields, axis=1)
    
    columns = list(X.columns.values)
    
    # Standard scaler
    sc_X = StandardScaler()
    X = sc_X.fit_transform(X)
    
    # Select top 20 features
    estimator = ltb.LGBMRegressor()
    #rfe = RFE(estimator=estimator, n_features_to_select=40, step=10, verbose=1)
    rfe = RFECV(estimator=estimator, min_features_to_select=30, step=10, cv=4, n_jobs=1, verbose=1)
    rfe.fit(X, y)
    features = rfe.get_support(indices = True) 
    
    np.array(columns)[features]
    
    """
    ['SI_MRKT_WSE_0', 'MECHERO_0', 'ABP_0', 'DISPENSADOR_ESP_0',
       'VISIT_0', 'MECHERO_1', 'ABP_1', 'DISPENSADOR_ESP_1', 'VISIT_1',
       'CLIPPER_2', 'ABP_2', 'DISPENSADOR_ESP_2', 'CUE_2', 'VISIT_2',
       'MECHERO_3', 'CLIPPER_3', 'DISPENSADOR_ESP_3', 'CUE_3', 'VISIT_3',
       'MECHERO_4', 'DISPENSADOR_ESP_4', 'VISIT_4', 'MECHERO_5',
       'CLIPPER_5', 'ABP_5', 'DISPENSADOR_ESP_5', 'AZAFATA_5',
       'TOTEM_ESP_5', 'SVM_5', 'CUE_5', 'QUOTA_SELLIN_0',
       'QUOTA_SELLIN_1', 'QUOTA_SELLIN_2', 'QUOTA_SELLIN_3',
       'QUOTA_SELLIN_4', 'sellin_itg_var', 'sellin_itg_std',
       'sellin_mrkt_var', 'sellin_mrkt_std', 'qouta_sellin_var',
       'qouta_sellin_std', 'MECHERO_var', 'MECHERO_std', 'CLIPPER_var',
       'CLIPPER_std', 'ABP_var', 'ABP_std', 'DISPENSADOR_var',
       'AZAFATA_var', 'TOTEM_var', 'TOTEM_ESP_var', 'SVM_var', 'CUE_var',
       'VISIT_var', 'VISIT_std', 'SEASON', 'SEASON_SPRING',
       'SEASON_SUMMER']
    """
    
    best_features = list(np.array(columns)[features])
    
    X = data[best_features+['CUSTOMER_ID', 'BRANDFAMILY_ID', 'QUOTA_SELLOUT_5'] + date_cols]
    
    np.save(OUTPUT_FEATS, list(X.columns.values))

else:
    features = list(np.load(OUTPUT_FEATS))
    X = data[features]


X.to_hdf(OUTPUT, key='prepared_train', index=False, mode='w')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))