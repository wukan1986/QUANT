# -*- coding: utf-8 -*-

import pandas as pd
from tools import get_tradeDay
import datetime
import numpy as np


def get_data(ret_path, today):
    df_ret = pd.read_csv('%s/CNE5_LOCALID_%s.DSRT' % (ret_path, today[2:]), skiprows=1)
    df_ret = df_ret[[u'BARRID', u'LOCID', u'SPRET%']]

    df_temp = pd.read_csv('%s/CNE5S_LOCALID_%s.RSK' % (ret_path, today[2:]), skiprows=1)
    df_temp = df_temp[df_temp['ESTU'] == 1]
    df_temp = df_temp[[u'BARRID', u'LOCID', u'ESTU',u'SRISK%',u'PRICE']]

    df_total = pd.merge(df_temp, df_ret, on='BARRID')

    df_total = df_total[['LOCID_x', 'SPRET%', 'SRISK%', 'PRICE']]
    df_total.columns = ['Symbol', 'spret','srisk','price']
    df_total['Symbol'] = df_total['Symbol'].apply(lambda x: x[2:] + '-CN')
    return df_total

def get_sret_5(ret_path, day5):
    df = pd.DataFrame([])
    for i in day5:
        df_temp = get_data(ret_path, i)
        df_temp['date'] = i
        df = pd.concat([df, df_temp])
    sret = df.pivot(index='date',columns='Symbol',values='spret')
    sret = sret.mean()
    sret = sret.reset_index()
    sret.columns = ['Symbol','sret']
    return sret

def get_close_20(ret_path,day20):
    df = pd.DataFrame([])
    for i in day20:
        df_temp = get_data(ret_path, i)
        df_temp['date'] = i
        df = pd.concat([df, df_temp])
    close20 = df.pivot(index='date',columns='Symbol',values='price')
    close20 = close20.mean()
    close20 = close20.reset_index()
    close20.columns = ['Symbol', 'close']
    return close20


def run(ret_path, output_path, today):
    commission = 0.0023
    stamp_buy = 0.0001
    stamp_sell = 0.0001


    day = datetime.datetime.strptime(today, '%Y%m%d')
    pre_day = day - datetime.timedelta(days=40)
    day = day.strftime('%Y%m%d')
    pre_day = pre_day.strftime('%Y%m%d')

    tradeday = get_tradeDay.wind(pre_day, today, fre = 'day')
    day20 = tradeday.iloc[-20:]
    day5 = tradeday.iloc[-5:]


    srisk = get_data(ret_path, today)
    srisk = srisk[['Symbol','srisk']]

    sret5 = get_sret_5(ret_path, day5)
    close20 = get_close_20(ret_path,day20)

    df_tcost = pd.merge(srisk, sret5, on='Symbol')
    df_tcost = pd.merge(df_tcost, close20, on='Symbol')

    df_tcost['lincoeff'] = commission + 0.5*(stamp_buy + stamp_sell) + 2.5 * df_tcost['sret'] + 0.005/df_tcost['close']
    df_tcost['sqrtcoeff'] = 0.001*df_tcost['srisk']

    df_tcost = df_tcost[['Symbol','lincoeff','sqrtcoeff']]
    df_tcost = df_tcost.sort_values('Symbol')

    df_header = pd.DataFrame([[np.nan,'GROUP','GROUP'],['name','lincoeff','sqrtcoeff'],['unit','number','number'],[np.nan,np.nan,np.nan]],columns=['Symbol','lincoeff','sqrtcoeff'])

    df_final = pd.concat([df_header,df_tcost])
    df_final.to_csv('%s/coef_%s.csv'%(output_path,today),index=None, header= None)

if __name__ == '__main__':
    ret_path = 'Z:/MSCI/daily'
    output_path = 'Z:/daily_data/tcost_model'
    today = '20180110'
    run(ret_path, output_path, today)