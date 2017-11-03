# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 09:36:06 2017

@author: shiyunchao
"""

import turnover_class2
import numpy as np
import pandas as pd
import os
from time import strftime, localtime, time

import sys
reload(sys)
sys.setdefaultencoding('utf8')

class stom_backtest_update():
    def __init__(self,sql,flag,factor_filename1,factor_filename2,factor_filename3,day_range,trail_range3,\
                trail_range12,new_dirpath,new_factor_name):
        self.sql=sql       
        
        self.factor_filename1 = factor_filename1
        
        self.flag = flag
         
        self.day_range = day_range
              
        turnover = turnover_class2.factorGet(sql, flag,factor_filename1,day_range)
        turnover.backtest_or_update()
        
        
        self.dirpath = turnover.dirpath
        self.trade_day = turnover.trade_day[1:]
        if flag == 0:
            self.trade_day = self.trade_day.iloc[:-1]

#        self.today = strftime("%Y%m%d",localtime()) 
        self.today = self.trade_day.iloc[-2]
        
        self.factor_filename2 = factor_filename2
        self.trail_range3 = trail_range3
          
        self.factor_filename3 = factor_filename3 
        self.trail_range12 =  trail_range12 
    
        self.new_dirpath = new_dirpath  #  处理好的因子存放位置
        self.new_factor_name = new_factor_name
        
        if not os.path.exists(self.new_dirpath):
            os.mkdir(self.new_dirpath)
            
        if not os.path.exists(self.new_dirpath + '/' + self.new_factor_name):
            os.mkdir(self.new_dirpath + '/' + self.new_factor_name)

###############################################################################
#  3month

    def stom1(self, today, trail_range, trade_day, new_factorname, old_factorname, dirpath): 
        if not os.path.exists( dirpath + '/' + new_factorname):
            os.mkdir( dirpath + '/' + new_factorname)
        df = pd.DataFrame(columns = ['code'])
        trade_day_part = trade_day[trade_day<=today].iloc[-trail_range:]
        for day1 in trade_day_part:
            print(day1)
            path = dirpath + '/' + old_factorname + '/' + old_factorname \
            + '_raw_CN_' + day1 + '.csv'
            df1 = pd.read_csv(path)
            df1.columns = ['code',day1]
            df = pd.merge(df, df1, on='code', how='outer')
             
        df = df.set_index('code')   
        df = np.exp(df)  
        df = df.sum(axis=1)
        df = np.log(df/float(trail_range))
        df = df.replace(-np.inf, np.nan)
        newpath =  dirpath + '/' + new_factorname + '/' + new_factorname \
            + '_raw_CN_' + day1 + '.csv'
        df = pd.DataFrame(df)
        df = df.reset_index()
        df.columns = ['S_INFO_WINDCODE', '%s_raw' % new_factorname]
        df.to_csv(newpath,index=None)


    def update_stoq(self):
        if self.flag ==0:
            for i in range(self.trail_range3-1,len(self.trade_day)):
                today = self.trade_day.iloc[i]
                self.stom1(today, self.trail_range3, self.trade_day, self.factor_filename2, self.factor_filename1, self.dirpath)
        elif self.flag == 1:
            today = self.today
            self.stom1(today, self.trail_range3, self.trade_day, self.factor_filename2, self.factor_filename1, self.dirpath)

    def update_stoa(self):
        if self.flag ==0:
            for i in range(self.trail_range12-1,len(self.trade_day)):
                today = self.trade_day.iloc[i]
                self.stom1(today, self.trail_range12, self.trade_day, self.factor_filename3, self.factor_filename1, self.dirpath)
        elif self.flag == 1:
            today = self.today
            self.stom1(today, self.trail_range12, self.trade_day, self.factor_filename3, self.factor_filename1, self.dirpath)

    
    def cal_liquidity(self, today, dirpath, factor_filename1 ,factor_filename2, factor_filename3,\
                        new_dirpath, new_factor_name):
        path1 = dirpath + '/' + factor_filename1 + '/' + factor_filename1 + '_raw_CN_' + today + '.csv'
        path2 = dirpath + '/' + factor_filename2 + '/' + factor_filename2 + '_raw_CN_' + today + '.csv'
        path3 = dirpath + '/' + factor_filename3 + '/' + factor_filename3 + '_raw_CN_' + today + '.csv'
        df1 = pd.read_csv(path1)
        df2 = pd.read_csv(path2)
        df3 = pd.read_csv(path3)
        df1.columns = ['code','stom']
        df2.columns = ['code','stoq']
        df3.columns = ['code','stoa']
        df = pd.merge(df1, df2, on='code', how='outer')
        df = pd.merge(df, df3, on='code', how='outer')
        
        df = df.set_index('code')
        df =df.fillna(0)
        df[new_factor_name] = 0.35*df['stom'] + 0.35*df['stoq'] + 0.3*df['stoa']
        df = df[[new_factor_name]] 
        newpath =  new_dirpath + '/' + new_factor_name + '/' + new_factor_name \
                + '_raw_CN_' + today + '.csv'
        df = pd.DataFrame(df)
        df = df.reset_index()
        df.columns = ['S_INFO_WINDCODE', '%s_raw' % new_factor_name]
        df.to_csv(newpath,index = None)


    def update_liquidity(self):
        if self.flag ==0:
            for k in self.trade_day[self.trail_range12-1:]:
                print(k)
                self.cal_liquidity(k, self.dirpath, self.factor_filename1 ,self.factor_filename2, self.factor_filename3,\
                                self.new_dirpath, self.new_factor_name)
        elif self.flag == 1:   
            today = self.today
            self.cal_liquidity(today, self.dirpath, self.factor_filename1 ,self.factor_filename2, self.factor_filename3,\
                                self.new_dirpath, self.new_factor_name)
                            

if __name__ == "__main__": 
    sql="select s_info_windcode,trade_dt,s_dq_freeturnover from AShareEODDerivativeIndicator"       
    factor_filename1 = 'STOM'
    flag = 0 # 0回测 1 更新
    day_range = 21
    factor_filename2 = 'STOQ'
    trail_range3 =  3#12
      
    factor_filename3 = 'STOA'
    trail_range12 =  12#12
    
    new_dirpath = 'data'   #  处理好的因子存放位置
    new_factor_name = 'LIQUIDITY'
    
    test = stom_backtest_update(sql,flag,factor_filename1,factor_filename2,factor_filename3,day_range,trail_range3,\
                    trail_range12,new_dirpath,new_factor_name)
                    
    test.update_stoq()
    test.update_stoa()
    test.update_liquidity()               
################################################################################
'''
上面的可以用于更新了
'''

#def stom(trail_range, trade_day, new_factorname, old_factorname, dirpath):
#    if not os.path.exists( dirpath + '/' + new_factorname):
#        os.mkdir( dirpath + '/' + new_factorname)
#        
#    for i in range(trail_range-1,len(trade_day)):
#        df = pd.DataFrame(columns = ['code'])
#        for j in range(trail_range):
#            day1 = trade_day.iloc[i-j] 
#            print(day1)
#            path = dirpath + '/' + old_factorname + '/' + old_factorname \
#            + '_' + day1 + '.csv'
#            df1 = pd.read_csv(path,header =None)
#            df1.columns = ['code','lag%s'%j]
#            df = pd.merge(df, df1, on='code', how='outer')
#    
#        df = df.set_index('code')   
#        df = np.exp(df)  
#        df = df.sum(axis=1)
#        df = np.log(df/float(trail_range))
#        df = df.replace(-np.inf, np.nan)
#        newpath =  dirpath + '/' + new_factorname + '/' + new_factorname \
#            + '_' + trade_day.iloc[i] + '.csv'
#        df.to_csv(newpath,header=None)
#
#
#
#dirpath = turnover.dirpath
#trade_day = turnover.trade_day[1:]
#factor_filename2 = 'stoq'
#trail_range =  3#12
#stom(trail_range, trade_day, factor_filename2, factor_filename1, dirpath)
#
#
#factor_filename3 = 'stoa'
#trail_range =  12#12
#stom(trail_range, trade_day, factor_filename3, factor_filename1, dirpath)
#



#new_dirpath = 'data'   #  处理好的因子存放位置
#new_factor_name = 'Liquidity'
#
#if not os.path.exists(new_dirpath):
#    os.mkdir(new_dirpath)
#    
#if not os.path.exists(new_dirpath + '/' + new_factor_name):
#    os.mkdir(new_dirpath + '/' + new_factor_name)
    
#for k in trade_day[11:]:
#
#    path1 = dirpath + '/' + factor_filename1 + '/' + factor_filename1 + '_' + k + '.csv'
#    path2 = dirpath + '/' + factor_filename2 + '/' + factor_filename2 + '_' + k + '.csv'
#    path3 = dirpath + '/' + factor_filename3 + '/' + factor_filename3 + '_' + k + '.csv'
#    df1 = pd.read_csv(path1,header =None)
#    df2 = pd.read_csv(path2,header =None)
#    df3 = pd.read_csv(path3,header =None)
#    df1.columns = ['code','stom']
#    df2.columns = ['code','stoq']
#    df3.columns = ['code','stoa']
#    df = pd.merge(df1, df2, on='code', how='outer')
#    df = pd.merge(df, df3, on='code', how='outer')
#    
#    df = df.set_index('code')
#    df =df.fillna(0)
#    df[new_factor_name] = 0.35*df['stom'] + 0.35*df['stoq'] + 0.3*df['stoa']
#    
#    newpath =  new_dirpath + '/' + new_factor_name + '/' + new_factor_name \
#            + '_' + k + '.csv'    
#    df.to_csv(newpath)




'''
下面里面出现nan的原因是因为 turnover 1month里面最后把负无穷替换为缺失值了  要不要替换？？？

0.35*df['stom'] + 0.35*df['stoq'] + 0.3*df['stoa']   现在的做法是把nan用0填充  然后相加    还是有nan的

最后把负无穷替换为np.nan  还是保留！！！！！！
'''

