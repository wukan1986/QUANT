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

start = '20170901'
end = strftime("%Y%m%d", localtime())

dirpath = 'data'

update = False
# sql = "select TRADE_DT,S_INFO_WINDCODE,S_DQ_ADJPRECLOSE,S_DQ_ADJOPEN,S_DQ_ADJHIGH,S_DQ_ADJLOW,S_DQ_ADJCLOSE,S_DQ_TRADESTATUS,S_DQ_ADJFACTOR from ASHAREEODPRICES where TRADE_DT>=%s and TRADE_DT<=%s"%(start,end)
# getdb = client_db.read_db()
# df = getdb.db_query(sql)


if not os.path.exists(dirpath):
    os.mkdir(dirpath)

fre = 'day'
getdb = client_db.read_db()
tradeday = get_tradeDay.wind(start, end, fre=fre)
tradeday = tradeday.iloc[:-1]
today = tradeday.iloc[-1]


def run(day, dirpath):
    sql = "select TRADE_DT,S_INFO_WINDCODE,S_DQ_ADJPRECLOSE,S_DQ_ADJOPEN,S_DQ_ADJHIGH,S_DQ_ADJLOW,S_DQ_ADJCLOSE,S_DQ_TRADESTATUS from ASHAREEODPRICES where TRADE_DT=%s" % (
        day)

    df = getdb.db_query(sql)
    df.columns = ['trade_day', 'code', 'adjpreclose', 'adjopen', 'adjhigh', 'adjlow', 'adjclose','status']
    df['status'] = df['status'].apply(lambda x: x.decode('gbk'))
    df.to_csv('%s/%s.csv' % (dirpath, day), index=None,encoding='gb18030')


# for day in tradeday:
#    print(day)
#    run(day,dirpath)
#    break

def split(L, s):
    return [L[i:i + s] for i in range(len(L)) if i % s == 0]


if update:
    run(today, dirpath)
else:
    part_day = split(tradeday.values.tolist(), 10)

    for dd in part_day:
        ls_th = []
        for day in dd:
            t = threading.Thread(target=run, args=(day, dirpath))
            ls_th.append(t)
        for t in ls_th:
            t.start()
        for t in ls_th:
            t.join()
