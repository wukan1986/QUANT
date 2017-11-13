# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 16:48:16 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime
import numpy as np
import datetime
import uqer
from uqer import DataAPI
client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')


def replaceCode(x):
    if 'XSHE' in x:
        return x.replace('XSHE','SZ')
    elif 'XSHG' in x:
        return x.replace('XSHG','SH')
        

def getIC(path, today, n_days):
    print('calculate IC')
    filename = os.listdir(path)
    
    filename1 = pd.Series(filename)
    filename1 = filename1.apply(lambda x: x.split('.')[0])
    
    filename1 = filename1[(filename1>n_days)&(filename1<today)]
    
    trade_day = filename1.values.tolist()
    
    col = ['BETA','MOMENTUM','SIZE','EARNYILD','RESVOL','GROWTH','BTOP','LEVERAGE','LIQUIDTY','SIZENL']
    
    df = pd.DataFrame([])
    
    i = 0
    for i in range(len(trade_day)-1):
#        print(i)
        t = trade_day[i]
        t1 = trade_day[i+1]
        
        ret = DataAPI.MktEqudGet(tradeDate=t1,secID=u"",ticker=u"",\
            beginDate=u"",endDate=u"",isOpen="",field=['secID','chgPct'],pandas="1")
        
        ret['secID'] = ret['secID'].apply(replaceCode)
        
        factor = pd.read_csv(path+t+'.csv',encoding = 'gb18030')    
        factor = factor[['secID']+col]
        
        df_merge = pd.merge(ret, factor, on='secID')
        df_merge = df_merge.set_index('secID')
        df_corr = df_merge.corr('spearman')
        df_corr = df_corr.iloc[0][col]
        df_corr = pd.DataFrame(df_corr)
        df_corr.columns = [t] 
        df = pd.concat([df, df_corr],axis = 1)
    
    df = df.T
    df.to_csv('data/IC.csv')
    print('calculate IC  Done!')


if __name__ == "__main__":
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=-32)
    n_days = now + delta
    today = now.strftime('%Y%m%d')
    n_days = n_days.strftime('%Y%m%d')
    end = strftime("%Y-%m-%d",localtime()) 
    
    dirpath = '../database/short'
    
    path = '%s/RMExposure/'%dirpath
    getIC(path, today, n_days)