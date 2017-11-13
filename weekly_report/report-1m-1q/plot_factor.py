# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 16:48:16 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime
import numpy as np
#import seaborn as sns
import matplotlib.pylab as plt
import matplotlib
matplotlib.style.use('ggplot')
from pylab import mpl   #画图显示中文
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


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




def getRetPlot(path, start, end):
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
    
    df.tradeDate = pd.to_datetime(df.tradeDate, format='%Y%m%d')
    df.tradeDate = df.tradeDate.apply(lambda x: x.strftime("%Y-%m-%d"))
    df = df.set_index('tradeDate')
    
    date = strftime("%Y-%m-%d",localtime())
    firstday = date[:4] + '-01-01'
    df_year = df[(df.index>=firstday) & (df.index<=date)]   # 年初到现在
    df_q = df.iloc[-60:]  #df_q = df.iloc[-71:-6]#
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
    return df_2week, df_month, df_q, df_year, col1, col2



def getPng(df_ret, col_list, title, filename, angle):
    rett = df_ret.iloc[-1][col_list]- 1
    fig = plt.figure(figsize=(20, 10))
    ax1 = fig.add_subplot(111)
    rett_df = pd.DataFrame(rett*100)  # the x locations for the groups
    rett_df.columns = ['Return(%)']
    
    rett_df.reset_index(inplace = True)
    rett_df['index'] = rett_df['index'].replace(egh2chs)
    rett_df = rett_df.set_index('index')
    rett_df = rett_df.sort_values('Return(%)', axis=0, ascending=False)
    
    rett_df['Num'] = range(len(rett))
    
    ret_plus = rett_df[rett_df['Return(%)']>0]['Return(%)']
    ret_minus = rett_df[rett_df['Return(%)']<0]['Return(%)']
    ax1.bar(rett_df[rett_df['Return(%)']>0]['Num'].values, ret_plus.values, align='center', color='r', width=0.35)
    ax1.bar(rett_df[rett_df['Return(%)']<0]['Num'].values, ret_minus.values, align='center', color='g', width=0.35)
    
    ax1.set_xlim(left=-0.5, right=len(rett)-0.5)
    ax1.set_ylabel(u'累计收益率', fontsize=24)
    ax1.set_xticks(rett_df['Num'].values)
    ax1.set_xticklabels(list(rett_df.index), rotation=angle, fontsize=22, color='black')
    ax1.set_yticklabels([str(x)+'0%' for x in ax1.get_yticks()], fontsize=22, color='black')
    ax1.set_title(u"%s"%title, fontsize=26)
    plt.savefig('data/%s.png'%filename)
    plt.close()



def getPng2(df_ic, title, filename, angle):

    fig = plt.figure(figsize=(20, 10))
    ax1 = fig.add_subplot(111)
    rett_df = pd.DataFrame(df_ic)  # the x locations for the groups
    rett_df.columns = ['Return(%)']
    
    rett_df.reset_index(inplace = True)
    rett_df['index'] = rett_df['index'].replace(egh2chs)
    rett_df = rett_df.set_index('index')
    rett_df = rett_df.sort_values('Return(%)', axis=0, ascending=False)
    
    rett_df['Num'] = range(len(rett_df))
    
    ret_plus = rett_df[rett_df['Return(%)']>0]['Return(%)']
    ret_minus = rett_df[rett_df['Return(%)']<0]['Return(%)']
    ax1.bar(rett_df[rett_df['Return(%)']>0]['Num'].values, ret_plus.values, align='center', color='r', width=0.35)
    ax1.bar(rett_df[rett_df['Return(%)']<0]['Num'].values, ret_minus.values, align='center', color='g', width=0.35)
    
    ax1.set_xlim(left=-0.5, right=len(rett_df)-0.5)
    ax1.set_ylabel(u'IC', fontsize=24)
    ax1.set_xticks(rett_df['Num'].values)
    ax1.set_xticklabels(list(rett_df.index), rotation=angle, fontsize=22, color='black')
    ax1.set_yticklabels([str(x) for x in ax1.get_yticks()], fontsize=22, color='black')
    ax1.set_title(u"%s"%title, fontsize=26)
    plt.savefig('data/%s.png'%filename)
    plt.close()
    
def run(path, start, end):
    df_2week, df_month, df_q, df_year, col1, col2 = getRetPlot(path, start, end)
    
    getPng(df_month, col1, u"Barra风险因子 最近1个月", 'factor_month', 35)
    getPng(df_q, col1, u"Barra风险因子 最近1个季度", 'factor_year', 35)
    
    getPng(df_month, col2, u"申万行业因子 最近1个月", 'industry_month', 45)
    getPng(df_q, col2, u"申万行业因子 最近1个季度", 'industry_year',45)
    
    df_ic = pd.read_csv('data/IC.csv',index_col=0)
    getPng2(df_ic.mean(), u'Barra风险因子 最近1个月IC', 'IC_month', 30)
    getPng2(df_ic.mean()/df_ic.std(), u'Barra风险因子 最近1个月IR', 'IR_month', 30)
     #当天的IC随机性太大  没意义
    #getPng2(df_ic.iloc[-1], u'Barra风险因子  当前IC', 'IC_month_now', 30)   

if __name__ == "__main__":
    start = '20161001'
    #end = strftime("%Y-%m-%d",localtime()) 
    end = '20170820'
    
    dirpath = '../database/short'
    
    path = '%s/RMFactorRet/'%dirpath
    
    run(path, start, end)


