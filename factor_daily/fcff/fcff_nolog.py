# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 09:34:52 2017

@author: shiyunchao
"""


import factor_fundmental1
from factor_daily import factor_fundmental
import pandas as pd
import numpy as np
import os
from time import strftime, localtime, time

today = strftime("%Y%m%d",localtime()) #'20170929'
today = '20171106'
###############################################################################
#sql = "select WIND_CODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,net_profit_excl_min_int_inc from ashareincome"
sql="select WIND_CODE,ANN_DT,REPORT_PERIOD,s_fa_fcff from AShareFinancialIndicator"
factor_filename1 = 'fcff'

cal_mode1,flag = 'ttm', 0 # 0 回测 1更新

fre_shift_list = range(6)

for fre_shift in fre_shift_list:
    net_income = factor_fundmental1.factorGet(sql, cal_mode1,flag,factor_filename1, fre_shift, date = today, fre='day')
    # asd1 = net_income.backtest_or_update()

###############################################################################
sql = "select WIND_CODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,tot_assets from AShareBalanceSheet"

factor_filename2 = 'tot_asset'

cal_mode2,flag = 'year_ave', 0 # 0 回测 1更新

fre_shift = 0
fre_shift_list = range(6)
for fre_shift in fre_shift_list:
    tot_asset = factor_fundmental.factorGet(sql, cal_mode2,flag,factor_filename2, fre_shift, date = today, fre='day')
    asd2 = tot_asset.backtest_or_update()

raw_dirpath = 'raw_data'

new_dirpath = 'data'   #  处理好的因子存放位置
if not os.path.exists(new_dirpath):
    os.mkdir(new_dirpath)

new_factor_name = 'fcftar'

trade_day = net_income.trade_day
#trade_day = ['20170126','20170228','20170331','20170428','20170531','20170630','20170731','20170831']
################################################################################        
def update(i,file1,factor_filename1,cal_mode1,file2,factor_filename2,cal_mode2):
    path1 = file1+ '/' + factor_filename1 + '_' + cal_mode1 + '_' + i + '.csv'
    path2 = file2+ '/' + factor_filename2 + '_' + cal_mode2 + '_' + i + '.csv'

    df1 = pd.read_csv(path1,header=None)
    df2 = pd.read_csv(path2,header=None)
    df1.columns = ['code','factor1']
    df2.columns = ['code','factor2']

    df = pd.merge(df1,df2, on='code', how='outer')
    df = df.replace({'Na':np.nan,'Ne':np.nan})
    df['factor1'] = df['factor1'].astype('float64')
    df['factor2'] = df['factor2'].astype('float64')
    df['factor'] = df['factor1']/df['factor2']
    df = df[['code','factor']]

    df['code'] = df['code'].apply(lambda x: x.split('.')[0] + '-CN')

    df.to_csv('%s/%s_%s.csv'%(new_factor_path,new_factor_name,i),index=None,header=None)

for fre in fre_shift_list:
    file1 = raw_dirpath + '/' + factor_filename1 + '_' + cal_mode1 + '_' + str(fre)
    file2 = raw_dirpath + '/' + factor_filename2 + '_' + cal_mode2 + '_' + str(fre)

    new_factor_path = new_dirpath + '/' + new_factor_name + str(fre)
    if not os.path.exists(new_factor_path):
        os.mkdir(new_factor_path)
    if flag==0:
        for i in trade_day:
            update(i,file1,factor_filename1,cal_mode1,file2,factor_filename2,cal_mode2)
    elif flag ==1:
#        today = strftime("%Y%m%d",localtime())
        update(today,file1,factor_filename1,cal_mode1,file2,factor_filename2,cal_mode2)



######################
def updatestd(ii,factor_num,new_dirpath,new_factor_name,new_factor_path2,new_factor_name2):
    df_total = pd.DataFrame([],columns = ['code'])
    for num in factor_num:
        new_factor_path = new_dirpath + '/' + new_factor_name + str(num)

        path = new_factor_path + '/' + new_factor_name + '_' + str(ii) + '.csv'
        df = pd.read_csv(path,header=None)
        df.columns = ['code','factor'+str(num)]
        df_total = pd.merge(df,df_total, on='code', how='outer')
    df_total.set_index('code',inplace = True)
    stdni = df_total.std(axis=1)

    stdni = stdni[stdni != 0]
    stdni = np.log(stdni)
    stdni = -stdni

    stdni = pd.DataFrame(stdni)
    stdni.reset_index(inplace=True)
    stdni.columns = ['S_INFO_WINDCODE', '%s_raw' % new_factor_name2.upper()]
    stdni.to_csv('%s/%s_%s.csv' % (new_factor_path2.upper(), new_factor_name2.upper(), ii), index=None, header=None)


new_factor_name2 = 'stdfcf0'
new_factor_path2 = new_dirpath + '/' + new_factor_name2

if not os.path.exists(new_factor_path2):
    os.mkdir(new_factor_path2)

factor_num = range(0,5)

if flag==0:
    for ii in trade_day:
        updatestd(ii,factor_num,new_dirpath,new_factor_name,new_factor_path2,new_factor_name2)
elif flag==1:
    updatestd(today,factor_num,new_dirpath,new_factor_name,new_factor_path2,new_factor_name2)

## 000015 数据异常  2000年后就退市了 

new_factor_name2 = 'stdfcf1'
new_factor_path2 = new_dirpath + '/' + new_factor_name2

if not os.path.exists(new_factor_path2):
    os.mkdir(new_factor_path2)

factor_num = range(1,6)

if flag==0:
    for ii in trade_day:
        updatestd(ii,factor_num,new_dirpath,new_factor_name,new_factor_path2,new_factor_name2)
elif flag==1:
    updatestd(today,factor_num,new_dirpath,new_factor_name,new_factor_path2,new_factor_name2)
