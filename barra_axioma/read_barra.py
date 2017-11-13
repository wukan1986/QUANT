# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 10:31:12 2017

@author: shiyunchao
"""


import numpy as np
import pandas as pd
import os
from sklearn.preprocessing import OneHotEncoder
import sys
sys.path.append("..")
from tools import get_tradeDay
reload(sys)


def get_filename(tradeday):
    exposure_list = []
    cov_list = []
    for day in tradeday:
        if day > '20140930':
            cov_list.append('CNE5S%s.COV'%day[2:])
            exposure_list.append('CNE5S_X_%s.RSK'%day[2:])
        else:
            cov_list.append('CNE5S%s.COV' % day[2:6])
            exposure_list.append('CNE5S%s.RSK' % day[2:6])
    return exposure_list,cov_list


def get_exposure(dirpath,filename,output,day):
    print(day)
    factor_col = ['BETA','MOMENTUM','SIZE','EARNYILD','RESVOL','GROWTH','BTOP','LEVERAGE','LIQUIDTY','SIZENL']
    ind_col = ['ENERGY  ', 'CHEM    ', 'CONMAT  ', 'MTLMIN  ', 'MATERIAL',
           'AERODEF ', 'BLDPROD ', 'CNSTENG ', 'ELECEQP ', 'INDCONG ',
           'MACH    ', 'TRDDIST ', 'COMSERV ', 'AIRLINE ', 'MARINE  ',
           'RDRLTRAN', 'AUTO    ', 'HOUSEDUR', 'LEISLUX ', 'CONSSERV',
           'MEDIA   ', 'RETAIL  ', 'PERSPRD ', 'BEV     ', 'FOODPROD',
           'HEALTH  ', 'BANKS   ', 'DVFININS', 'REALEST ', 'SOFTWARE',
           'HDWRSEMI', 'UTILITIE']
    ind_col2 = list(map(lambda x: x.strip(),ind_col))

    df = pd.read_csv('%s/%s'%(dirpath,filename),skiprows=1)
    df = df[df['ESTU']==1]

    df = df[df['LOCID']!='CHNFBC1 ']

    df['LOCID'] = df['LOCID'].apply(lambda x: x.split('CN')[1] + '-CN')
    df['SRISK%'] = df['SRISK%']/100
    df_factor = df[['LOCID']+factor_col]


    y = df['IND'].values
    y.shape = (len(y), 1)
    ysample = OneHotEncoder().fit_transform(y)
    y = ysample.toarray()

    df_ind = pd.DataFrame(y,columns = ind_col2)
    df_ind['LOCID'] = df['LOCID'].values

    df_exposure = pd.merge(df_factor, df_ind, on='LOCID')
    df_exposure['COUNTRY'] = df['COUNTRY'].values

    df_exposure = df_exposure.merge(df[['LOCID','SRISK%']], on='LOCID')

    df_exposure = df_exposure[['LOCID'] + factor_col + ind_col2 + ['COUNTRY'] + ['SRISK%']]

    col = [np.nan] + df_exposure.columns.tolist()[1:-1] + [np.nan]
    df_tmp = pd.DataFrame([col, col], columns=df_exposure.columns)
    df_new = pd.concat([df_tmp, df_exposure])

    df_new.to_csv('%s/Exposure_%s.csv'%(output,day),index=None,header=None)


def get_cov(dirpath,filename,output,day):
    df = pd.read_csv('%s/%s'%(dirpath,filename),skiprows=2)
    df = df.iloc[:,1:-1]
    col_cov = list(map(lambda x: x.strip(),df.columns.tolist()))
    df.columns = col_cov
    df = df/10000.0
    df.index = col_cov
    df = df.reset_index()
    df.columns = [np.nan] + df.columns.tolist()[1:]
    df.to_csv('%s/Covariance_%s.csv'%(output,day),index=None)


dirpath = 'Z:/barradata'
output1 = 'Z:/axioma_data/barra_model/Exposure'
output2 = 'Z:/axioma_data/barra_model/Covariance'

start = '20060101'
end = '20170510'

if not os.path.exists(output1):
    os.mkdir(output1)
if not os.path.exists(output2):
    os.mkdir(output2)

fre = 'month'
tradeday = get_tradeDay.wind(start, end, fre=fre)

exposure_list,cov_list = get_filename(tradeday)

for i in range(len(exposure_list)):
    day = tradeday.iloc[i]
    get_exposure(dirpath,exposure_list[i],output1,day)
    get_cov(dirpath,cov_list[i],output2,day)