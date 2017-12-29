# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 10:31:12 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime
from tools import get_tradeDay


def update(save_day, factor_name, factor_name1, dir_path, output_path):
    df = pd.read_csv('%s/Exposure_%s.csv'%(dir_path,save_day),skiprows=1)

    df = df[['Unnamed: 0'] + factor_name1]
    df = df.set_index('Unnamed: 0')
    df = -df
    for j in range(len(factor_name)):
        df_temp = df[[factor_name1[j]]]
        df_temp = df_temp.reset_index()
        df_temp.columns = ['S_INFO_WINDCODE', '%s_raw' % factor_name[j]]

        df_temp.to_csv('%s/%s/%s_raw_CN_%s.csv' % (output_path, factor_name[j], factor_name[j], save_day), index=None)


def run(output_path, start, end, flag, fre, dir_path):

    factor_name1 = ['Style.SIZE', 'Style.RESVOL', 'Style.SIZENL', 'Style.LIQUIDTY']
    factor_name = ['SIZE_barra', 'RESVOL_barra', 'SIZENL_barra', 'LIQUIDTY_barra']


    for i in factor_name:
        if not os.path.exists(output_path + '/' + i):
            os.mkdir(output_path + '/' + i)


    trade_day = get_tradeDay.wind(start, end, fre=fre)

    if flag == 0:
        for i in range(len(trade_day)):
            today = trade_day.iloc[i]
            save_day = today
            print(save_day)
            update(save_day, factor_name, factor_name1, dir_path, output_path)
    elif flag == 1:
        save_day = end
        print(save_day)
        update(save_day, factor_name, factor_name1, dir_path, output_path)


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
    dir_path = 'Z:/daily_data/barra_model/Exposure'
    output_path = 'Z:/axioma_data/alpha'
    output_path = 'Z:/daily_data/alpha/raw'

    start = '20171201'
    fre = 'day'
    flag = 0  # 0 回测 1更新

    run(output_path, start, today, flag,fre,dir_path)
