
# Import the libraries
import pandas as pd
import numpy as np
import datetime
import dask.dataframe as dd
import time

t1 = time.time()

def get_season(M) : 
    list1 = [[12 , 1 , 2], [3 , 4 , 5],  
             [6 , 7 , 8], [9 , 10 , 11]] 
    season = None
    if M in list1[0] : 
        season = 0 
    elif M in list1[1] : 
        season = 1
    elif M in list1[2] : 
        season = 2 
    elif M in list1[3] : 
        season = 3 
    else : 
        season = -1 
    
    return season

INPUT = '../Data/cleaned_train.h5'
OUTPUT = '../Data/featured_train.h5'

# Read merged data for sampling
X = pd.read_hdf(INPUT, key='cleaned_train')

#############################################################################################################

# SELLIN Derived Features

#############################################################################################################

X['sellin_itg_mean'] = X[['SI_ITG_WSE_4','SI_ITG_WSE_3','SI_ITG_WSE_2','SI_ITG_WSE_1', 'SI_ITG_WSE_0']].mean(axis=1)
X['sellin_itg_var'] = X[['SI_ITG_WSE_4','SI_ITG_WSE_3','SI_ITG_WSE_2','SI_ITG_WSE_1', 'SI_ITG_WSE_0']].var(axis=1)
X['sellin_itg_std']  = X[['SI_ITG_WSE_4','SI_ITG_WSE_3','SI_ITG_WSE_2','SI_ITG_WSE_1', 'SI_ITG_WSE_0']].std(axis=1)
X['sellin_itg_sum'] = X[['SI_ITG_WSE_4','SI_ITG_WSE_3','SI_ITG_WSE_2','SI_ITG_WSE_1', 'SI_ITG_WSE_0']].sum(axis=1)

X['sellin_mrkt_mean'] = X[['SI_MRKT_WSE_4','SI_MRKT_WSE_3','SI_MRKT_WSE_2','SI_MRKT_WSE_1','SI_MRKT_WSE_0']].mean(axis=1)
X['sellin_mrkt_var'] = X[['SI_MRKT_WSE_4','SI_MRKT_WSE_3','SI_MRKT_WSE_2','SI_MRKT_WSE_1','SI_MRKT_WSE_0']].var(axis=1)
X['sellin_mrkt_std'] = X[['SI_MRKT_WSE_4','SI_MRKT_WSE_3','SI_MRKT_WSE_2','SI_MRKT_WSE_1','SI_MRKT_WSE_0']].std(axis=1)
X['sellin_mrkt_sum'] = X[['SI_MRKT_WSE_4','SI_MRKT_WSE_3','SI_MRKT_WSE_2','SI_MRKT_WSE_1','SI_MRKT_WSE_0']].sum(axis=1)

X['qouta_sellin_mean'] = X[['QUOTA_SELLIN_4','QUOTA_SELLIN_3','QUOTA_SELLIN_2','QUOTA_SELLIN_1','QUOTA_SELLIN_0']].mean(axis=1)
X['qouta_sellin_var'] = X[['QUOTA_SELLIN_4','QUOTA_SELLIN_3','QUOTA_SELLIN_2','QUOTA_SELLIN_1','QUOTA_SELLIN_0']].var(axis=1)
X['qouta_sellin_std'] = X[['QUOTA_SELLIN_4','QUOTA_SELLIN_3','QUOTA_SELLIN_2','QUOTA_SELLIN_1','QUOTA_SELLIN_0']].std(axis=1)
X['qouta_sellin_sum'] = X[['QUOTA_SELLIN_4','QUOTA_SELLIN_3','QUOTA_SELLIN_2','QUOTA_SELLIN_1','QUOTA_SELLIN_0']].sum(axis=1)

#############################################################################################################

# Inversions Derived Features

#############################################################################################################

inv = ['MECHERO', 'CLIPPER', 'ABP', 'DISPENSADOR', 'VISIBILIDAD', 'VISIBILIDAD_ESP', 'AZAFATA', 'TOTEM', 'TOTEM_ESP', 'SVM', 'TFT', 'CUE', 'VISIT']
for i in inv:
    X[i+'_mean'] = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].mean(axis=1)
    X[i+'_var'] = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].var(axis=1)
    X[i+'_std']  = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].std(axis=1)
    X[i+'_sum'] = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].sum(axis=1)

#############################################################################################################

# Season Derived Features

#############################################################################################################

X['SEASON'] = X['CAL_DATE_5'].apply(lambda x: get_season(x.month))

X['SEASON_WINTER'] = X['SEASON'].apply(lambda x: 1 if x == 0 else 0)
X['SEASON_SPRING'] = X['SEASON'].apply(lambda x: 1 if x == 1 else 0)
X['SEASON_SUMMER'] = X['SEASON'].apply(lambda x: 1 if x == 2 else 0)
X['SEASON_AUTUMN'] = X['SEASON'].apply(lambda x: 1 if x == 3 else 0)

"""

#############################################################################################################

# SELLOUT Derived Features

#############################################################################################################

X['sellout_itg_mean'] = X[['SO_ITG_WSE_4','SO_ITG_WSE_3','SO_ITG_WSE_2','SO_ITG_WSE_1', 'SO_ITG_WSE_0']].mean(axis=1)
X['sellout_itg_var'] = X[['SO_ITG_WSE_4','SO_ITG_WSE_3','SO_ITG_WSE_2','SO_ITG_WSE_1', 'SO_ITG_WSE_0']].var(axis=1)
X['sellout_itg_std'] = X[['SO_ITG_WSE_4','SO_ITG_WSE_3','SO_ITG_WSE_2','SO_ITG_WSE_1', 'SO_ITG_WSE_0']].std(axis=1)
X['sellout_itg_sum']  = X[['SO_ITG_WSE_4','SO_ITG_WSE_3','SO_ITG_WSE_2','SO_ITG_WSE_1', 'SO_ITG_WSE_0']].sum(axis=1)

X['sellout_mrkt_mean'] = X[['SO_MRKT_WSE_4','SO_MRKT_WSE_3','SO_MRKT_WSE_2','SO_MRKT_WSE_1', 'SO_MRKT_WSE_0']].mean(axis=1)
X['sellout_mrkt_var'] = X[['SO_MRKT_WSE_4','SO_MRKT_WSE_3','SO_MRKT_WSE_2','SO_MRKT_WSE_1', 'SO_MRKT_WSE_0']].var(axis=1)
X['sellout_mrkt_std'] = X[['SO_MRKT_WSE_4','SO_MRKT_WSE_3','SO_MRKT_WSE_2','SO_MRKT_WSE_1', 'SO_MRKT_WSE_0']].std(axis=1)
X['sellout_mrkt_sum'] = X[['SO_MRKT_WSE_4','SO_MRKT_WSE_3','SO_MRKT_WSE_2','SO_MRKT_WSE_1', 'SO_MRKT_WSE_0']].sum(axis=1)

X['qouta_sellout_mean'] = X[['QUOTA_SELLOUT_4','QUOTA_SELLOUT_3','QUOTA_SELLOUT_2','QUOTA_SELLOUT_1','QUOTA_SELLOUT_0']].mean(axis=1)
X['qouta_sellout_var'] = X[['QUOTA_SELLOUT_4','QUOTA_SELLOUT_3','QUOTA_SELLOUT_2','QUOTA_SELLOUT_1','QUOTA_SELLOUT_0']].var(axis=1)
X['qouta_sellout_std'] = X[['QUOTA_SELLOUT_4','QUOTA_SELLOUT_3','QUOTA_SELLOUT_2','QUOTA_SELLOUT_1','QUOTA_SELLOUT_0']].std(axis=1)
X['qouta_sellout_sum'] = X[['QUOTA_SELLOUT_4','QUOTA_SELLOUT_3','QUOTA_SELLOUT_2','QUOTA_SELLOUT_1','QUOTA_SELLOUT_0']].sum(axis=1)

"""

X.to_hdf(OUTPUT, key='featured_train', index=False, mode='w')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))