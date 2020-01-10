
# Import the libraries

import pandas as pd
import numpy as np
import pyodbc 
import os 
from tqdm import tqdm 


OUTPUT= '../Data/Seasonality_Base.csv'

conn = pyodbc.connect('Driver={SQL Server};'
                       'Server=esmz08srdb009.imptobnet.com;'
                       'Database=ITE_PRD;'
                       'Trusted_Connection=yes;')
 
cursor = conn.cursor()

query = 'select * from [dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY order by customer_id, brandFamily_id, year_id, SACA_ID'

sql = '\n' + query 

generator = pd.read_sql(sql,conn, chunksize=100000)
     
begin = True
for chunk in tqdm(generator):
    if begin:
        chunk.to_csv(OUTPUT, sep='|', index=False, mode='a')
        begin = False
    else:
        chunk.to_csv(OUTPUT, sep='|', index=False, mode='a', header=False)
