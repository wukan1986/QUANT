# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 11:30:50 2017

@author: shiyunchao
"""

import pandas as pd
import sys
import os
import threading
from time import strftime, localtime, time
sys.path.append("..")
from tools import client_db, get_tradeDay
reload(sys)


start = '20060101'
end = strftime("%Y%m%d",localtime())

dirpath = 'data'

update = False
#sql = "select TRADE_DT,S_INFO_WINDCODE,S_DQ_TRADESTATUS from ASHAREEODPRICES where TRADE_DT>=%s and TRADE_DT<=%s"%(start,end)

if not os.path.exists(dirpath):
    os.mkdir(dirpath)

fre = 'day'
getdb = client_db.read_db()
tradeday = get_tradeDay.wind(start, end, fre = fre) 
tradeday = tradeday.iloc[:-1]
today = tradeday.iloc[-1]

day = tradeday[0]

def run(day,dirpath):
    sql = "select TRADE_DT,S_INFO_WINDCODE,S_DQ_ADJPRECLOSE,S_DQ_ADJHIGH,S_DQ_ADJLOW,S_DQ_TRADESTATUS from ASHAREEODPRICES where TRADE_DT=%s"%(day)
    
    df = getdb.db_query(sql)
    df['S_DQ_TRADESTATUS'] = df['S_DQ_TRADESTATUS'].apply(lambda x: x.decode('gbk'))
    
    df['zhangting'] = df['S_DQ_ADJLOW']/df['S_DQ_ADJPRECLOSE'] - 1
    df['zhangting'] = df['zhangting'] >= 9.9/100.0
    
    df['dieting'] = df['S_DQ_ADJHIGH']/df['S_DQ_ADJPRECLOSE'] - 1
    df['dieting'] = df['dieting'] <= -9.9/100.0
    
    df['tingpai'] = df['S_DQ_TRADESTATUS']==u'停牌' 
    
    df1 = df[df['tingpai'] | df['zhangting'] | df['dieting']]
    
    df1 = df1[['S_INFO_WINDCODE']]
    
    df1.to_csv('%s/%s.csv'%(dirpath,day),index=None)

#for day in tradeday:
#    print(day)
#    run(day,dirpath)

def split(L,s):
    return [L[i:i+s] for i in range(len(L)) if i%s==0]
    
if update:
    run(today,dirpath)
else:
    part_day = split(tradeday.values.tolist(),10)    
    
    for dd in part_day:
        ls_th =[]
        for day in dd:
            t = threading.Thread(target=run,args=(day,dirpath))
            ls_th.append(t)
        for t in ls_th:
            t.start()
        for t in ls_th:
            t.join()






