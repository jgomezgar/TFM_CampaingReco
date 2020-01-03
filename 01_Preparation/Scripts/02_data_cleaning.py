
# Import the libraries
import pandas as pd
import numpy as np
import time

t1 = time.time()

INPUT = '../Data/sampled_train.h5'
OUTPUT = '../Data/cleaned_train.h5'

# Read merged data for sampling
X = pd.read_hdf(INPUT, key='sampled_train')

nan_values = (X.isna().sum())

# Removing data with inf or nan values
X.replace([np.inf, -np.inf], np.nan, inplace=True)
X.dropna(inplace=True)

X = X[X['QUOTA_SELLOUT_5'] >= 0]

# Export the cleaned data to a h5
X.to_hdf(OUTPUT, key='cleaned_train', index=False, mode='w')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))