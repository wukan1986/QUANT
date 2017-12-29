# -*- coding: utf-8 -*-

from uqer import DataAPI
import os
from tools import get_tradeDay
from time import strftime, localtime, time
import pandas as pd

from xutils import (Date,
                    Calendar,
                    Period)

def run(firstRun_update, start, end, fre, index_code, dir_path, beachmark_file):
    if not os.path.exists('%s' % beachmark_file):
        os.mkdir('%s' % beachmark_file)
    if not os.path.exists('%s/%s' % (beachmark_file, index_code)):
        os.mkdir('%s/%s' % (beachmark_file, index_code))
    if firstRun_update == 0:
        trade_day = get_tradeDay.wind(start, end, fre)
        for i in trade_day:
            print(i)
            df = pd.read_excel('%s/000846weightnextday%s.xls'%(dir_path,i), dtype={u'成分券代码\nConstituent Code':str})
            df = df.iloc[:,[4,16]]
            df.columns = ['code','weight']
            df['code'] = df['code'].apply(lambda x: x+'-CN')
            df.to_csv('%s/%s/benchmark_%s.csv' % (beachmark_file, index_code, i), index=None, header=None)
    elif firstRun_update == 1:
        cal = Calendar('China.SSE')
        today = strftime("%Y%m%d", localtime())
        today = Date.strptime(today, '%Y%m%d')
        today = cal.advanceDate(today, Period('-1b'))
        today = today.strftime("%Y%m%d")
        df = pd.read_excel('%s/000846weightnextday%s.xls' % (dir_path, today), dtype={u'成分券代码\nConstituent Code': str})
        df = df.iloc[:, [4, 16]]
        df.columns = ['code', 'weight']
        df['code'] = df['code'].apply(lambda x: x + '-CN')
        df.to_csv('%s/%s/benchmark_%s.csv' % (beachmark_file, index_code, today), index=None, header=None)

if __name__ == '__main__':

    dir_path = 'G:/ESG100Enhanced/ZZZS'
    start = '20171001'
    end = strftime("%Y%m%d", localtime())

    firstRun_update = 1 ####  0代表第一次运行  1代表更新

    fre = 'day'
    beachmark_file = 'Z:/daily_data/benchmark'
    index_code1 = u"000846"
    run(firstRun_update, start, end, fre, index_code1, dir_path, beachmark_file)
















