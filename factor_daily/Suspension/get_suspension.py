# -*- coding: utf-8 -*-


import pandas as pd
import sys
import os
import threading
from time import strftime, localtime, time
from xutils import (Date,
                    Calendar,
                    Period)

sys.path.append("..")
from tools import client_db, get_tradeDay
reload(sys)

def suspend(day, dirpath, getdb):
    sql = 'select S_INFO_WINDCODE from wind.AShareTradingSuspension t where S_DQ_SUSPENDDATE = %s and S_DQ_RESUMPDATE is null'%(day)
    df = getdb.db_query(sql)
    df = df.drop_duplicates()
    df['value'] = 1
    df['S_INFO_WINDCODE'] = df['S_INFO_WINDCODE'].apply(lambda x: x.split('.')[0] + '-CN')

    cal = Calendar('China.SSE')
    day_temp = Date.strptime(day, '%Y%m%d')
    day_temp = cal.advanceDate(day_temp, Period('-1b'))
    day = day_temp.strftime("%Y%m%d")

    df.to_csv('%s/suspended_%s.csv' % (dirpath, day), index=None,header=None)

def run(start, end, today, dirpath, flag, fre):
    getdb = client_db.read_db()
    tradeday = get_tradeDay.wind(start, end, fre=fre)
    tradeday = tradeday.iloc[:-1]
    if not os.path.exists(dirpath):
       os.mkdir(dirpath)
    if flag==0:
        for day in tradeday:
           print(day)
           suspend(day,dirpath, getdb)
    else:
        suspend(today, dirpath, getdb)



if __name__ == '__main__':
    start = '20171130'
    end = strftime("%Y%m%d", localtime())
    today = strftime("%Y%m%d", localtime())
    dirpath = 'Z:/daily_data/suspended'

    fre = 'day'
    flag = 1
    run(start, end, today, dirpath, flag, fre)

# def split(L, s):
#     return [L[i:i + s] for i in range(len(L)) if i % s == 0]
#
#
# if update:
#     run(today, dirpath)
# else:
#     part_day = split(tradeday.values.tolist(), 10)
#
#     for dd in part_day:
#         ls_th = []
#         for day in dd:
#             t = threading.Thread(target=run, args=(day, dirpath))
#             ls_th.append(t)
#         for t in ls_th:
#             t.start()
#         for t in ls_th:
#             t.join()