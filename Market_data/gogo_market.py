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
end = strftime("%Y%m%d", localtime())

dirpath = 'gogo_data'

update = False

# day = '20171031'
# sql = "select SYMBOL,LTDR from P_GG_KEYDATA where TDATE=%s" % day
# test = client_db.read_db(type='gogo')
# df = test.db_query(sql)


if not os.path.exists(dirpath):
    os.mkdir(dirpath)

fre = 'month'
getdb = client_db.read_db(type='gogo')
tradeday = get_tradeDay.wind(start, end, fre=fre)
# tradeday = tradeday.iloc[:-1]
today = tradeday.iloc[-1]


def run(day, dirpath):
    sql = "select SYMBOL,LTDR from P_GG_KEYDATA where TDATE=%s" % (day)

    df = getdb.db_query(sql)
    df.columns = ['ticker','close_s']
    df.to_csv('%s/close_%s.csv' % (dirpath, day), index=None)


for day in tradeday:
   print(day)
   run(day,dirpath)


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
