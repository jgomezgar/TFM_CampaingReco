# -*- coding: utf-8 -*-

# Import the libraries
import pandas as pd
import numpy as np
import pyodbc 
import os 
from tqdm import tqdm 
import sys

OUTPUT= '../Data/customers_ok15.csv'

conn = pyodbc.connect('Driver={SQL Server};'
                       'Server=esmz08srdb009.imptobnet.com;'
                       'Database=ITE_PRD;'
                       'Trusted_Connection=yes;')
 
cursor = conn.cursor()

query = open('../02_TT_Customer_OK_15M.sql', 'r') .read()

sql = '\n' + query 

data = pd.read_sql(sql,conn)

data.to_csv(OUTPUT, sep='|', index=False, mode='a')
     