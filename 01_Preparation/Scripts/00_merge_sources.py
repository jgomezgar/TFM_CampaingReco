
# Import the libraries
import pandas as pd
import numpy as np
import dask.dataframe as dd
import time

t1 = time.time()

INPUT_UNIFIED = '../../00_Extraction/Data/Sales_and_Invest.csv'
INPUT_CLUSTERS = '../02_AuxData/Data_Clusters_lavels.csv'
OUTPUT =  '../Data/merged_sources.h5'

# Read unified data and merge with the clusters information
data = dd.read_csv(INPUT_UNIFIED, sep='|').compute()
clusters = dd.read_csv(INPUT_CLUSTERS, sep='|').compute()

if 'Unnamed: 0' in list(data.columns):
    data.drop('Unnamed: 0', axis=1, inplace=True)

data = data.merge(clusters[['customer_id', 'label']], left_on='CUSTOMER_ID', right_on='customer_id', how='left')
#data = data.merge(clusters, left_on='CUSTOMER_ID', right_on='customer_id', how='left')
data = data.drop(['Label', 'Label_desc', 'medalla'], axis=1)

# Calculate quota sellin and quota sellout
for i in range (0, 6):
    data['QUOTA_SELLIN_'+str(i)] = np.divide(data['SI_ITG_WSE_'+str(i)].astype(np.float)*100, data['SI_MRKT_WSE_'+str(i)].astype(np.float))
    data['QUOTA_SELLOUT_'+str(i)] = np.divide(data['SO_ITG_WSE_'+str(i)].astype(np.float)*100, data['SO_MRKT_WSE_'+str(i)].astype(np.float))

"""
# Export the merged data to a csv
data.to_csv(OUTPUT, sep='|', index=False)
"""
data.to_hdf(OUTPUT, key='merged', mode='w')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))