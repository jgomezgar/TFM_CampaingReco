
# Import the libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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
si_itg_wse = ['SI_ITG_WSE_5', 'SI_ITG_WSE_4','SI_ITG_WSE_3','SI_ITG_WSE_2','SI_ITG_WSE_1', 'SI_ITG_WSE_0']

X['sellin_itg_mean'] = X[si_itg_wse].mean(axis=1)
X['sellin_itg_var'] = X[si_itg_wse].var(axis=1)
X['sellin_itg_std']  = X[si_itg_wse].std(axis=1)
X['sellin_itg_sum'] = X[si_itg_wse].sum(axis=1)

si_mkrt_wse = ['SI_MRKT_WSE_5', 'SI_MRKT_WSE_4','SI_MRKT_WSE_3','SI_MRKT_WSE_2','SI_MRKT_WSE_1','SI_MRKT_WSE_0']

X['sellin_mrkt_mean'] = X[si_mkrt_wse].mean(axis=1)
X['sellin_mrkt_var'] = X[si_mkrt_wse].var(axis=1)
X['sellin_mrkt_std'] = X[si_mkrt_wse].std(axis=1)
X['sellin_mrkt_sum'] = X[si_mkrt_wse].sum(axis=1)

quota_sellin = ['QUOTA_SELLIN_5', 'QUOTA_SELLIN_4','QUOTA_SELLIN_3','QUOTA_SELLIN_2','QUOTA_SELLIN_1','QUOTA_SELLIN_0']

X['qouta_sellin_mean'] = X[quota_sellin].mean(axis=1)
X['qouta_sellin_var'] = X[quota_sellin].var(axis=1)
X['qouta_sellin_std'] = X[quota_sellin].std(axis=1)
X['qouta_sellin_sum'] = X[quota_sellin].sum(axis=1)

#############################################################################################################

# Inversions Derived Features

#############################################################################################################

inv = ['MECHERO', 'CLIPPER', 'ABP', 'DISPENSADOR', 'VISIBILIDAD', 'VISIBILIDAD_ESP', 'AZAFATA', 'TOTEM', 'TOTEM_ESP', 'SVM', 'TFT', 'CUE', 'VISIT']
inv += ['PERC_MECHERO', 'PERC_CLIPPER', 'PERC_ABP', 'PERC_DISPENSADOR', 'PERC_VISIBILIDAD', 'PERC_VISIBILIDAD_ESP', 'PERC_AZAFATA', 'PERC_TOTEM', 'PERC_TOTEM_ESP', 'PERC_SVM', 'PERC_TFT', 'PERC_CUE', 'PERC_visit']
for i in inv:
    X[i+'_mean'] = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].mean(axis=1)
    X[i+'_var'] = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].var(axis=1)
    X[i+'_std']  = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].std(axis=1)
    X[i+'_sum'] = X[[i+'_4',i+'_3',i+'_2',i+'_1', i+'_0']].sum(axis=1)

#############################################################################################################

# Season Derived Features

#############################################################################################################

X['SEASON'] = X['CAL_DATE_5'].apply(lambda x: get_season(datetime.datetime.strptime(x, "%Y-%m-%d").month))

X['SEASON_WINTER'] = X['SEASON'].apply(lambda x: 1 if x == 0 else 0)
X['SEASON_SPRING'] = X['SEASON'].apply(lambda x: 1 if x == 1 else 0)
X['SEASON_SUMMER'] = X['SEASON'].apply(lambda x: 1 if x == 2 else 0)
X['SEASON_AUTUMN'] = X['SEASON'].apply(lambda x: 1 if x == 3 else 0)

X.to_hdf(OUTPUT, key='featured_train', index=False, mode='w')

t2 = time.time()

print ("Time to execute script:",str(t2-t1))