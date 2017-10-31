# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 13:56:58 2017

@author: shiyunchao
"""


import pandas as pd
import os 
import sys
sys.path.append("..")
from tools import get_tradeDay
reload(sys)

dirpath = 'Z:/axioma_data/alpha'
filterpath = 'data'
output = 'data_filter'

factor_name = ['liquqer']

start = '20061201'
end = '20171012'

if not os.path.exists(output):
    os.mkdir(output)
        
fre = 'month'
tradeday = get_tradeDay.wind(start, end, fre = fre) 

for factor in factor_name:
    if not os.path.exists('%s/%s'%(output,factor)):
        os.mkdir('%s/%s'%(output,factor))
    for day in tradeday:
        df = pd.read_csv('%s/%s/%s_raw_CN_%s.csv'%(dirpath, factor, factor, day),header=None)
        df.columns = ['code','factor']        
        df_filter = pd.read_csv('%s/%s.csv'%(filterpath,day))
        df_filter.columns = ['code']
        df_filter['code'] = df_filter['code'].apply(lambda x: x.split('.')[0] + '-CN')
        df1 = df[~df['code'].isin(df_filter['code'])]
        df1.to_csv('%s/%s/%s_raw_CN_%s.csv'%(output, factor, factor, day),index=None,header=None)


