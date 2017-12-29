# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 09:19:36 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime
import uqer
from uqer import DataAPI
client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')

import sys
sys.path.append("F:/QUANT/")
reload(sys)
from tools import get_tradeDay

def update(save_day, factor_name, factor_name1, output_path):
    df = DataAPI.RMExposureDayGet(secID=u"", ticker=u"", tradeDate="%s" % save_day, beginDate=u"", endDate=u"", field=u"",
                                  pandas="1")
    if df.empty:
        df = DataAPI.RMExposureDayGet(secID=u"", ticker=u"", tradeDate="%s" % save_day, beginDate=u"", endDate=u"",
                                      field=u"", pandas="1")


    df = df[['ticker'] + factor_name1]
    df['ticker'] = df['ticker'].apply(lambda x: x + '-CN')
    df = df.set_index('ticker')
    df = -df
    for j in range(len(factor_name)):
        df_temp = df[[factor_name1[j]]]
        df_temp = df_temp.reset_index()
        df_temp.columns = ['S_INFO_WINDCODE', '%s_raw' % factor_name[j]]

        df_temp.to_csv('%s/%s/%s_raw_CN_%s.csv' % (output_path, factor_name[j], factor_name[j], save_day), index=None)


def run(output_path, start, end, flag, fre):
    factor_name = ['SIZE_uqer', 'RESVOL_uqer', 'SIZENL_uqer', 'LIQUIDTY_uqer']
    factor_name1 = ['SIZE', 'RESVOL', 'SIZENL', 'LIQUIDTY']

    for i in factor_name:
        if not os.path.exists(output_path + '/' + i):
            os.mkdir(output_path + '/' + i)


    trade_day = get_tradeDay.wind(start, end, fre=fre)

    if flag == 0:
        for i in range(len(trade_day)):
            today = trade_day.iloc[i]
            save_day = today
            print(save_day)
            update(save_day, factor_name, factor_name1, output_path)
    elif flag == 1:
        save_day = end
        print(save_day)
        update(save_day, factor_name, factor_name1,output_path)


if __name__ == '__main__':
    from xutils import (Date,
                        Calendar,
                        Period)

    cal = Calendar('China.SSE')

    today = strftime("%Y%m%d", localtime())
    today = Date.strptime(today, '%Y%m%d')
    today = cal.advanceDate(today, Period('-1b'))
    today = today.strftime("%Y%m%d")

    # output_path = 'Z:/axioma_data/alpha'
    output_path = 'F:/factor_data/test_data'

    start = '20170901'
    fre = 'month'
    flag = 0  # 0 回测 1更新

    run(output_path, start, today, flag,fre)
