
# Import the libraries

import pandas as pd
import numpy as np
import pyodbc 
import os 
from tqdm import tqdm 
import sys

OUTPUT = '../Data/sellout_data.csv'

conn = pyodbc.connect('Driver={SQL Server};'
                       'Server=esmz08srdb009.imptobnet.com;'
                       'Database=ITE_PRD;'
                       'Trusted_Connection=yes;')
 
cursor = conn.cursor()

query = open('../01_TT_Sales_and_Invest_Pivot6Times_10d.sql', 'r') .read()

sql = '\n' + query 

chunksize = 100000
generator = pd.read_sql(sql,conn, chunksize=chunksize)
     
begin = True
for chunk in tqdm(generator):
    if begin:
        chunk.to_csv(OUTPUT, sep='|', index=False, mode='a')
        begin = False
    else:
        chunk.to_csv(OUTPUT, sep='|', index=False, mode='a', header=False)
    
        
