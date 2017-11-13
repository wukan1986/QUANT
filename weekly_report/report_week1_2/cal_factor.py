# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 16:48:16 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime
import numpy as np
from datetime import datetime

    
egh2chs={'BETA':u'贝塔',
'MOMENTUM':u'动量',
'SIZE':u'规模',
'EARNYILD':u'盈利能力',
'RESVOL':u'波动率',
'GROWTH':u'成长',
'BTOP':u'估值',
'LEVERAGE':u'杠杆',
'LIQUIDTY':	u'流动性',
'SIZENL':u'非线性规模',
'Bank':u'银行',
'RealEstate':u'房地产',
'Health':u'医药生物',
'Transportation':u'交通运输',
'Mining':u'采掘',
'NonFerMetal':u'有色金属',
'HouseApp':u'家用电器',
'LeiService':u'休闲服务',
'MachiEquip':u'机械设备',
'BuildDeco':u'建筑装饰',
'CommeTrade':u'商业贸易',
'CONMAT':u'建筑材料',
'Auto':u'汽车',
'Textile':u'纺织服装',
'FoodBever':u'食品饮料',
'Electronics':u'电子',
'Computer':u'计算机',
'LightIndus':u'轻工制造',
'Utilities':u'公用事业',
'Telecom':u'通信',
'AgriForest':u'农林牧渔',
'CHEM':u'化工',
'Media':u'传媒',
'IronSteel':u'钢铁',
'NonBankFinan':u'非银金融',
'ELECEQP':u'电气设备',
'AERODEF':u'国防军工',
'Conglomerates':u'综合',
'COUNTRY':u'国家'}


def get_retfactor(path, start, end):
    filename = os.listdir(path)
    #filename1 = map(lambda x: x.split('.csv')[0], filename)
    filename = pd.Series(filename)
    filename = filename[(filename>'%s.csv'%start)&(filename<'%s.csv'%end)]
    
    df = pd.DataFrame([])
    i = filename.iloc[0]
    for i in filename:
        data = pd.read_csv(path+i)
        data['tradeDate'] = [i.split('.')[0]]
        df = pd.concat([df,data])
    del df['updateTime']
    print df
    df.tradeDate = pd.to_datetime(df.tradeDate, format='%Y%m%d')
    df.tradeDate = df.tradeDate.apply(lambda x: x.strftime("%Y-%m-%d"))
    df = df.set_index('tradeDate')
    
    date = strftime("%Y-%m-%d",localtime())
    firstday = date[:4] + '-01-01'
    df_year = df[(df.index>=firstday) & (df.index<=date)]   # 年初到现在
    df_q = df.iloc[-60:]
    df_month = df.iloc[-20:]
    df_2week = df.iloc[-10:]
    
    ret = df.copy() 
    df = (df+1).cumprod()
    df = df.round(3)
    
    ret_month = df_month.copy() 
    df_month = (df_month+1).cumprod()
    df_month = df_month.round(3)
    
    ret_year = df_year.copy()
    df_year = (df_year+1).cumprod()
    df_year = df_year.round(3)
    
    ret_q = df_q.copy()
    df_q = (df_q+1).cumprod()
    df_q = df_q.round(3)
    
    ret_2week = df_2week.copy()
    df_2week = (df_2week + 1).cumprod()
    df_2week = df_2week.round(3)
    
    
    col = df.columns.tolist()
    col1 = col[:10]
    col2 = col[10:]
    return ret_2week, ret_month, ret_q, ret_year, col1, col2

#cal_mdd = lambda x: (1 - x/x.cummax(axis=0)).max(axis=0)
#
#cal_sharpe = lambda x, y: (x.iloc[-1]-1)*(250/float(len(x)))/(y.std()*np.sqrt(250))


def indicator(ret_input, flag=0):
    cumret = (ret_input+1).cumprod()
#    cumret.plot()
    drawdown =  1 - cumret/cumret.cummax(axis=0)
    maxdrawdown = drawdown.max(axis=0)
    dd_day = drawdown[drawdown == maxdrawdown].index[0]
    
#    if flag == 0:
    ret_Y = cumret.iloc[-1] -1 
#    else:             
#    ret_Y = (cumret.iloc[-1] -1 )*(250/float(len(cumret)))
                       
    max_ret = (cumret[cumret.index<=dd_day]).max()
    
    ddmax_day = cumret[cumret==max_ret].index[0]
    
    dd_day = datetime.strptime(dd_day,'%Y-%m-%d')
    ddmax_day = datetime.strptime(ddmax_day,'%Y-%m-%d') 
    span = (dd_day-ddmax_day).days
    
#    if flag == 0:
#        sharpe = (cumret.iloc[-1]-1)/(ret_input.std()*np.sqrt(250))

    sharpe = (cumret.iloc[-1]-1)*(250/float(len(cumret)))/(ret_input.std()*np.sqrt(250))
    
    daily_min = ret_input.min()
    min_day = ret_input[ret_input==daily_min].index[0]
    
    win_ratio = (ret_input>0).sum()/float(len(ret_input))
    p2l_ratio = -1* ret_input[ret_input>0].sum()/(ret_input[ret_input<0].sum())
    
    result = [ret_Y, '%s%%'%(round(maxdrawdown,4)*100), span, round(sharpe,2), \
    '%s%%'%(round(daily_min,4)*100), min_day, '%s%%'%(round(win_ratio,3)*100), \
    round(p2l_ratio,2)]
    return result


def indicator2table(df_ret, col_list):
    df_data = pd.DataFrame(columns = ['Return', 'MDD','MDD_SPAN','Sharpe','Daily_Min','Min_Day','Win_Ratio','P2L_Ratio'])
    for i in col_list:
        print i 
        result = indicator(df_ret[i])
        
        df_data.ix[i] = result
#    df_data = df_data.to_json(force_ascii=False, orient='split')
    return df_data


def get_indicator(ret_month, col1, col2):
    df_test = indicator2table(ret_month, col1)
    df_test.reset_index(inplace = True)
    df_test['index'] = df_test['index'].replace(egh2chs)
    df_test = df_test.set_index('index')
    df_test = df_test.sort_values('Return', axis=0, ascending=False)
    
    df_test['Return'] = df_test['Return'].apply(lambda x: '%s%%'%(round(x,4)*100))
    df_test.to_csv('data/indicator_factor.csv', encoding = 'gb18030')
    
    
    df_test2 = indicator2table(ret_month, col2)
    df_test2.reset_index(inplace = True)
    df_test2['index'] = df_test2['index'].replace(egh2chs)
    df_test2 = df_test2.set_index('index')
    df_test2 = df_test2.sort_values('Return', axis=0, ascending=False)
    df_test2['Return'] = df_test2['Return'].apply(lambda x: '%s%%'%(round(x,4)*100))
    df_test2.to_csv('data/indicator_industry.csv', encoding = 'gb18030')

if __name__ == "__main__":
    start = '20141001'
    #end = strftime("%Y-%m-%d",localtime()) 
    end = '20170820'
    fre1 = '2week'
    dirpath = '../database/short'
    
    path = '%s/RMFactorRet/'%dirpath
    

    
    ret_2week, ret_month, ret_q, ret_year, col1, col2 = get_retfactor(path, start, end)
    get_indicator(ret_month, col1, col2)


