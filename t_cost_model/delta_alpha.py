# -*- coding: utf-8 -*-

import pandas as pd
from tools import get_tradeDay
import datetime
import numpy as np


def get_delta_alpha(output_path, today):

    # today = '20180108'
    # output_path = '.'
    index_list = ['ESG100','HS300','ZZ500']
    
    day = datetime.datetime.strptime(today, '%Y%m%d')
    pre_day = day - datetime.timedelta(days=20)
    day = day.strftime('%Y%m%d')
    pre_day = pre_day.strftime('%Y%m%d')
    
    trade_day = get_tradeDay.wind(pre_day, day, fre='day')
    pre_day = trade_day[trade_day < day].iloc[-1]
    
    df1 = pd.read_csv('Z:/daily_data/alpha/neut/alphaALL/alphaALL_neut_CN_%s.csv' % day, skiprows=3)
    df1.columns = [u'stock_code', u'alphaALL_T', u'alphaALL_ESG100_T', u'alphaALL_HS300_T',
                   u'alphaALL_ZZ500_T', u'Value_T', u'Quality_T', u'Revision_T', u'Fndsurp_T', u'IU_T',
                   u'Mktmmt_T', u'Insider_T', u'lincoef', u'sqrtcoef', u'UNIVERSE']
    df1 = df1[[u'stock_code', u'alphaALL_ESG100_T', u'alphaALL_HS300_T' , u'alphaALL_ZZ500_T']]
    df2 = pd.read_csv('Z:/daily_data/alpha/neut/alphaALL/alphaALL_neut_CN_%s.csv' % pre_day, skiprows=3)
    df2.columns = [u'stock_code', u'alphaALL_T-1', u'alphaALL_ESG100_T-1', u'alphaALL_HS300_T-1',
                   u'alphaALL_ZZ500_T-1', u'Value_T-1', u'Quality_T-1', u'Revision_T-1', u'Fndsurp_T-1', u'IU_T-1',
                   u'Mktmmt_T-1', u'Insider_T-1', u'lincoef', u'sqrtcoef', u'UNIVERSE']
    df2 = df2[[u'stock_code', u'alphaALL_ESG100_T-1', u'alphaALL_HS300_T-1' , u'alphaALL_ZZ500_T-1']]
    
    df = pd.merge(df1, df2, on='stock_code')
    
    
    for input1 in index_list:
        df['delta_alpha_%s'%input1] = df['alphaALL_%s_T' % input1] - df['alphaALL_%s_T-1' % input1]
    
    col = ['delta_alpha_%s'%i for i in index_list]
    df = df[['stock_code']+col]
    
    
    df_header = pd.DataFrame([[np.nan]+['GROUP']*len(col), ['name']+col, ['unit']+['number']*len(col),
                              [np.nan]+[np.nan]*len(col)], columns=['stock_code']+col)
    
    df_final = pd.concat([df_header, df])
    df_final.to_csv('%s/delta_alpha_%s.csv' % (output_path, today), index=None, header=None)

if __name__ == '__main__':
    start = '20180103'
    end='20180402'
    output_path = 'Z:/daily_data/tcost_model'
    trade_day = get_tradeDay.wind(start, end, fre='day')

    for today in trade_day:
        print(today)
        get_delta_alpha(output_path, today)




















