
# Import the libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time 
import dask.dataframe as dd

t1 = time.time()

INPUT = '../../00_Extraction/Data/Sales_and_Invest.csv'
INPUT_DATA = '../../00_Extraction/Data/sellout_data.csv'
INPUT_QUALITY = '../../00_Extraction/Data/customers_ok15.csv' 
OUTPUT_TRAIN = '../Data/sampled_train.h5'
OUTPUT_TEST = '../Data/sampled_test.h5'

X = dd.read_csv(INPUT_DATA, sep='|').compute()

# Get the differents customers and permutate them
customers = np.array(X['CUSTOMER_ID'].drop_duplicates())
index = np.random.permutation(len(customers))
customers = customers[index]

quality = True

if quality:
    #customers_ok = X['CUSTOMER_ID'][X['OK_15M'] == 1].drop_duplicates()
    customers_ok = pd.read_csv(INPUT_QUALITY, sep='|')['CUSTOMER_ID']
    train = customers_ok[:np.int(len(customers_ok)*0.4)].to_numpy()
    test = np.concatenate([customers_ok[np.int(len(customers_ok)*0.4):], X['CUSTOMER_ID'][X['OK_15M'] == 0].drop_duplicates()])

else:
    ratio = 0.3
    train = customers[:int(np.round(len(customers)*ratio))]
    test = customers[int(np.round(len(customers)*ratio)):]

index_train = X['CUSTOMER_ID'].isin(train)
index_test = X['CUSTOMER_ID'].isin(test)

X_train = X[index_train]
X_test = X[index_test]

# Export to hdf the two dataset sampled
X_train.to_hdf(OUTPUT_TRAIN, sep='|', key='sampled_train', index=False, mode='w')
X_test.to_hdf(OUTPUT_TEST, sep='|', key='sampled_test', index=False, mode='w')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))