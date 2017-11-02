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

dirpath = 'code_include'

update = False
# getdb = client_db.read_db()
# sql = "select S_INFO_CODE,S_INFO_LISTDATE,S_INFO_DELISTDATE,S_INFO_LISTBOARDNAME from ASHAREDESCRIPTION"
# df = getdb.db_query(sql)
# df.columns=['code', 'list_date', 'delist_date', 'list_board']
# df['list_board'] = df['list_board'].apply(lambda x: x.decode('gbk'))
#
# #两个作用: 去除上市未满一年的新股 & 去除没有上市日期的股票
# df1= df[df['list_date'] <= (str(int(end[:4])-1) + end[4:])]
# # 去除退市的股票
# df = df[df['delist_date'].isnull()]

if not os.path.exists(dirpath):
    os.mkdir(dirpath)

fre = 'day'
getdb = client_db.read_db()
tradeday = get_tradeDay.wind(start, end, fre=fre)
tradeday = tradeday.iloc[:-1]
today = tradeday.iloc[-1]

day = tradeday[0]

def get_info():
    sql = "select S_INFO_CODE,S_INFO_LISTDATE,S_INFO_DELISTDATE,S_INFO_LISTBOARDNAME from ASHAREDESCRIPTION"
    df = getdb.db_query(sql)
    df.columns = ['code', 'list_date', 'delist_date', 'list_board']
    df['list_board'] = df['list_board'].apply(lambda x: x.decode('gbk'))
    return df

def run(day, df, dirpath):
    # 两个作用: 去除上市未满一年的新股 & 去除没有上市日期的股票
    df = df[df['list_date'] <= (str(int(day[:4]) - 1) + day[4:])]
    # 去除退市的股票
    df = df[df['delist_date'].isnull()]
    df.to_csv('%s/%s.csv' % (dirpath, day), index=None, encoding='gb18030')

data = get_info()

for day in tradeday:
   print(day)
   run(day,data,dirpath)


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
