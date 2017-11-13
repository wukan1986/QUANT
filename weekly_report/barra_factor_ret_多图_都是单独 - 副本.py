# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 16:48:16 2017

@author: shiyunchao
"""

import os
import pandas as pd
from time import strftime, localtime

start = '20170101'
end = strftime("%Y%m%d",localtime()) 
#end = '20170820'
fre1 = '2week'
dirpath = 'E:/uqer_riskmodel/short'

path = '%s/RMFactorRet/'%dirpath

    
filename = os.listdir(path)
#filename1 = map(lambda x: x.split('.csv')[0], filename)
filename = pd.Series(filename)
filename = filename[(filename>'%s.csv'%start)&(filename<='%s.csv'%end)]

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

df_year = df.iloc[-240:]
df_month = df.iloc[-20:]

df = (df+1).cumprod()
df = df.round(3)

df_month = (df_month+1).cumprod()
df_month = df_month.round(3)

df_year = (df_year+1).cumprod()
df_year = df_year.round(3)


col = df.columns.tolist()
col1 = col[:10]
col2 = col[10:]

from pyecharts import Page
page = Page()

from pyecharts import Bar,Line
from pyecharts import Bar, Timeline, Grid

y_max = df[col1].max().max()
y_min = df[col1].min().min()

x1 = df.index.tolist()
line1 = Line(title = u"\nBarra风险因子 最近1年",width=1400, height= 600)
for i in col1:
    y1 = df[i].tolist()
#    line.add("%s"%i, x, y1, mark_line=["average"], mark_point=["max", "min"],yaxis_min= y_min, yaxis_max = y_max, is_datazoom_show=True, datazoom_range=[50, 80])
    line1.add("%s"%i, x1, y1, mark_point=["max"],yaxis_min= y_min, yaxis_max = y_max, is_datazoom_show=True, datazoom_range=[0, 100])
#line.show_config()



#grid = Grid()
#grid.add(line1, grid_top="55%")
#grid.add(line2, grid_bottom="55%", grid_left="55%")
#grid.add(line3, grid_bottom="55%", grid_right="55%")
#page.add(grid)

page.add(line1)



###############################################################################
y_max = df[col2].max().max()
y_min = df[col2].min().min()

x1 = df.index.tolist()
line4 = Line(title = u"\n\n\nBarra行业因子 最近1年",width=1400, height= 600)
for i in col2:
    y1 = df[i].tolist()
#    line.add("%s"%i, x, y1, mark_line=["average"], mark_point=["max", "min"],yaxis_min= y_min, yaxis_max = y_max, is_datazoom_show=True, datazoom_range=[50, 80])
    line4.add("%s"%i, x1, y1, mark_point=["max"],yaxis_min= y_min, yaxis_max = y_max, is_datazoom_show=True, datazoom_range=[0, 100])
#line.show_config()



page.add(line4)


page.render('factor.html')






