# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 10:00:29 2017

@author: shiyunchao
"""
import cal_factor
import cal_IC
import plot_factor
import datetime
from time import strftime, localtime, time

def step1():
    print('##########################################')
    print(u'计算风险收益指标')
    start = '20161001'
    end = strftime("%Y%m%d",localtime()) 
    #end = '20170820'     
    dirpath = 'E:/uqer_riskmodel/short'
    path = '%s/RMFactorRet/'%dirpath
    
    ret_2week, ret_month, ret_q, ret_year, col1, col2 = cal_factor.get_retfactor(path, start, end)
    cal_factor.get_indicator(ret_month, col1, col2)
    print(u'Done')
     
def step2():
    print('##########################################')
    print(u'计算风险因子最近一个月IC')
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=-32)
    n_days = now + delta
    today = now.strftime('%Y%m%d')
    n_days = n_days.strftime('%Y%m%d')

    dirpath = 'E:/uqer_riskmodel/short'
    
    path = '%s/RMExposure/'%dirpath
    cal_IC.getIC(path, today, n_days)
    print(u'Done')
    
    
def step3():
    print('##########################################')
    print(u'画收益率和IC直方图')   
    start = '20161001'
    end = strftime("%Y%m%d",localtime()) 
    #end = '20170820'
    
    dirpath = 'E:/uqer_riskmodel/short'
    
    path = '%s/RMFactorRet/'%dirpath
    
    plot_factor.run(path, start, end)
    print(u'Done')


   
step1()
step2()
step3()