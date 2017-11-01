# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 10:31:12 2017

@author: shiyunchao
"""

from datetime import datetime

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from pandas.plotting import table

matplotlib.style.use('ggplot')
# import seaborn as sns
# sns.set_style('white')
from pylab import mpl  # 画图显示中文

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
import yaml
import os
import axiomaBacktest

with open('E:/QUANT/R_code/config.yaml') as f:
    temp = yaml.load(f.read())
lag = int(temp['lag'])

dirpath = temp['dirpath']
outputpath = temp['outputpath']
market_path = temp['market_path']

if not os.path.exists(outputpath):
    os.makedirs(outputpath)
if not os.path.exists('%s/summary_excel' % dirpath):
    os.makedirs('%s/summary_excel' % dirpath)

factor = temp['factor_name']
factor = factor.split(',')

start_day = str(temp['start_day'])
end_day = str(temp['end_day'])
flag = temp['flag']
if flag == 1:
    risk_model = 'uqer_model'
else:
    risk_model = 'axioma_model'
strategy_name = temp['strategy_name']
fre = temp['frequency']
Benchmark = temp['Benchmark']
s = temp['start_day']
e = temp['end_day']

axiomaBacktest.get_ret_lag(factor, lag, dirpath, market_path, start_day, end_day)


def indicator(ret_input):
    ret_input = ret_input.dropna()
    cumret = (ret_input + 1).cumprod()
    drawdown = (1 - cumret / cumret.cummax(axis=0))
    maxdrawdown = drawdown.max()
    dd_day = drawdown[drawdown == maxdrawdown].index[0]
    #    cumret.plot()
    max_ret = (cumret[cumret.index <= dd_day]).max()
    ddmax_day = cumret[cumret == max_ret].index[0]

    dd_day = datetime.strptime(str(dd_day), '%Y%m%d')
    ddmax_day = datetime.strptime(str(ddmax_day), '%Y%m%d')
    span = (dd_day - ddmax_day).days

    ret_Y = (cumret.iloc[-1]) ** (12 / float(len(cumret))) - 1

    sharpe = ret_Y / (ret_input.std() * np.sqrt(12))  # 月收益率 * 根号12  日收益率 * 根号250

    ir = (12 * ret_input.mean()) / (ret_input.std() * np.sqrt(12))

    daily_min = ret_input.min()
    min_day = ret_input[ret_input == daily_min].index[0]

    win_ratio = (ret_input > 0).sum() / float(len(ret_input))
    p2l_ratio = -1 * ret_input[ret_input > 0].sum() / (ret_input[ret_input < 0].sum())

    return ret_Y, maxdrawdown, span, ir, sharpe, win_ratio, p2l_ratio


def get_info(df_summary):
    cumret = (df_summary['Period Total Return'] + 1).cumprod()
    t_ret = cumret.iloc[-1] - 1
    ret_Y = (cumret.iloc[-1]) ** (12 / float(len(cumret))) - 1

    cumret_b = (df_summary['Benchmark Return'] + 1).cumprod()
    ret_benchmark = (cumret_b.iloc[-1]) ** (12 / float(len(cumret_b))) - 1
    t_ret_benchmark = cumret_b.iloc[-1] - 1

    total_risk = df_summary['Total Risk'].mean()
    active_risk = np.sqrt(12) * df_summary['Active Return'].std()
    act_s_risk = df_summary['Active Specific Risk'].mean()
    turnover = df_summary['Turnover'].mean()
    turnover_ann = (df_summary['Turnover'].sum() / (len(df_summary['Turnover']))) * 12
    #    turnover_min = df_summary['Turnover'].iloc[1:].min()

    index_name = ['Annualized Active return', 'Total return', 'Annualized total return', 'Total Benchmark return',
                  'Annualized Benchmark return', \
                  'Total Risk(average)', 'Active Risk(average)', 'Active Specific Risk(average)', 'turnover(average)', \
                  'turnover_Annualized']

    data = [ret_Y - ret_benchmark, t_ret, ret_Y, t_ret_benchmark, ret_benchmark, total_risk, active_risk, act_s_risk,
            turnover, turnover_ann]

    df_info = pd.DataFrame(data, index=index_name, columns=['results'])

    df_info = df_info * 100
    df_info = df_info.round(2)

    df_info['results'] = df_info['results'].apply(lambda x: str(x) + '%')
    return df_info


for factor_name in factor:
    df_summary = pd.read_csv('%s/%sPeriod_Summary.csv' % (dirpath, factor_name), index_col=0)
    df_lag = pd.read_csv('%s/%s_lagret.csv' % (dirpath, factor_name), index_col=0)

    df_merge = pd.merge(df_lag, df_summary[['Benchmark Return']], how='left', left_index=True, right_index=True)

    #    df_merge = df_merge.dropna()
    df_ret = pd.DataFrame([])
    for i in range(lag + 1):
        df_ret['ActiveRet%s' % i] = df_merge['ret%s' % i] - df_merge['Benchmark Return'].shift(-i)
    df_ret.columns = ['group%s' % ii for ii in range(len(df_ret.columns))]
    df_ret_nan = df_ret.copy()
    df_ret = df_ret.fillna(0)
    ##################################
    #    df_ret = df_ret.iloc[:-1]
    #    df_ret = df_ret.fillna(0)

    df = pd.DataFrame(columns=['Ann_ActRet(%)', 'MDD(%)', 'MDD_SPAN', 'Ann_IR', 'Ann_Sharpe', 'Win_Ratio', 'P2L_Ratio'])

    for col in df_ret.columns.tolist():
        ret_Y, maxdrawdown, span, ir, sharpe, win_ratio, p2l_ratio = indicator(df_ret_nan[col])
        df.loc[col] = [ret_Y, maxdrawdown, span, ir, sharpe, win_ratio, p2l_ratio]

    # df.to_csv('indicator.csv')

    df_ret = df_ret.reset_index()
    df_ret['Period'] = pd.to_datetime(df_ret['Period'], format='%Y%m%d')
    df_ret.set_index('Period', inplace=True)

    cum_ret = (df_ret + 1).cumprod()
    df['Ann_ActRet(%)'] = df['Ann_ActRet(%)'] * 100
    df['MDD(%)'] = df['MDD(%)'] * 100
    df = df.round(2)

    ##############################################################################
    df_info = get_info(df_summary)
    df_info.loc['Frequency'] = fre
    df_info.loc['Risk model'] = risk_model
    df_info.loc['Benchamrk'] = Benchmark

    df_decay = df[['Ann_ActRet(%)', 'Ann_IR']]
    ##############################################################################
    # 约束条件信息
    con_col = ['Enabled', 'Min', 'Max', 'Type']
    df_constraint = pd.DataFrame([], columns=con_col)
    df_constraint.loc['budget', 'Enabled'] = str(temp['budget']['use'])
    df_constraint.loc['budget', 'Min'] = temp['budget']['Min']
    df_constraint.loc['budget', 'Max'] = temp['budget']['Max']
    df_constraint.loc['budget', 'Type'] = temp['budget']['Type']

    df_constraint.loc['stock_weight_limit', 'Enabled'] = str(temp['stock_weight_limit']['use'])
    df_constraint.loc['stock_weight_limit', 'Min'] = np.nan
    df_constraint.loc['stock_weight_limit', 'Max'] = temp['stock_weight_limit']['Max']
    df_constraint.loc['stock_weight_limit', 'Type'] = temp['stock_weight_limit']['Type']

    df_constraint.loc['TE_limit', 'Enabled'] = str(temp['TE_limit']['use'])
    df_constraint.loc['TE_limit', 'Min'] = np.nan
    df_constraint.loc['TE_limit', 'Max'] = temp['TE_limit']['Max']
    df_constraint.loc['TE_limit', 'Type'] = temp['TE_limit']['Type']

    df_constraint.loc['Limit_industry', 'Enabled'] = str(temp['Limit_industry']['use'])
    df_constraint.loc['Limit_industry', 'Min'] = temp['Limit_industry']['Min']
    df_constraint.loc['Limit_industry', 'Max'] = temp['Limit_industry']['Max']
    df_constraint.loc['Limit_industry', 'Type'] = temp['Limit_industry']['Type']

    df_constraint.loc['Limit_size', 'Enabled'] = str(temp['Limit_size']['use'])
    df_constraint.loc['Limit_size', 'Min'] = temp['Limit_size']['Min']
    df_constraint.loc['Limit_size', 'Max'] = temp['Limit_size']['Max']
    df_constraint.loc['Limit_size', 'Type'] = temp['Limit_size']['Type']

    df_constraint.loc['Benchmark_holding', 'Enabled'] = str(temp['Benchmark_holding']['use'])
    df_constraint.loc['Benchmark_holding', 'Min'] = temp['Benchmark_holding']['Min']
    df_constraint.loc['Benchmark_holding', 'Max'] = np.nan
    df_constraint.loc['Benchmark_holding', 'Type'] = temp['Benchmark_holding']['Type']
    ###############################################################################
    ### 保存统计信息

    writer = pd.ExcelWriter('%s/summary_excel/%s.xlsx' % (dirpath, factor_name))
    df_summary.to_excel(writer, 'original')
    df_ret_nan.to_excel(writer, 'Active Return')
    df.to_excel(writer, 'result')
    df_info.to_excel(writer, 'information')
    df_constraint.to_excel(writer, 'constraint')
    writer.save()

    ###############################################################################
    with PdfPages(u'%s/%s回测评价.pdf' % (outputpath, factor_name)) as pdf:
        fig, ax = plt.subplots(figsize=(12, 6))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, df_constraint, loc='best',
                      colWidths=[0.17] * len(df_constraint.columns))  # where summary_df is your data frame
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.set_fontsize(14)  # if ++fontsize is necessary ++colWidths
        tabla.scale(1.2, 2)  # change size table
        plt.subplots_adjust(top=0.8)
        ax.set_title(u"%s回测约束条件  %s - %s" % (factor_name, s, e), fontsize=14)
        pdf.savefig()
        plt.close()

        fig, ax = plt.subplots(figsize=(12, 6))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, df_info, loc='best',
                      colWidths=[0.09] * len(df_info.columns))  # where summary_df is your data frame
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.set_fontsize(14)  # if ++fontsize is necessary ++colWidths
        tabla.scale(5.5, 1.4)  # change size table
        plt.subplots_adjust(bottom=0, right=0.82)
        ax.set_title(u"%s当期回测信息  %s - %s" % (factor_name, s, e), fontsize=14)
        pdf.savefig()
        plt.close()

        cum_ret.plot(title=u'%s累计超额收益率' % factor_name, figsize=(12, 10), colormap='Set1')
        #    plt.subplots_adjust(bottom = 0.2)
        plt.legend(loc='best', fontsize=10)

        #    plt.subplots_adjust(right=0.85)
        pdf.savefig()
        plt.close()

        fig, ax = plt.subplots(figsize=(12, 4))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, df, loc='best', colWidths=[0.125] * len(df.columns))  # where summary_df is your data frame
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.set_fontsize(14)  # if ++fontsize is necessary ++colWidths
        tabla.scale(1.1, 1.8)  # change size table
        plt.subplots_adjust(bottom=0)
        ax.set_title(u"%s回测评价指标" % factor_name, fontsize=14)
        pdf.savefig()
        plt.close()

        df_decay.plot(kind='bar', title=u'%s因子衰减' % factor_name, figsize=(12, 5), subplots=True, layout=(1, 2))
        plt.legend(loc='best', fontsize=10)
        pdf.savefig()
        plt.close()


# df_summary
#
# def get_info(df_summary):
#    cumret = (df_summary['Period Total Return']+ 1).cumprod()  
#    t_ret = cumret.iloc[-1] - 1
#    ret_Y =  (cumret.iloc[-1])**(12/float(len(cumret))) - 1
#    
#    cumret_b = (df_summary['Benchmark Return']+ 1).cumprod()  
#    ret_benchmark =  (cumret_b.iloc[-1])**(12/float(len(cumret_b))) - 1
#    t_ret_benchmark = cumret_b.iloc[-1] - 1
#    
#    total_risk = df_summary['Total Risk'].mean()
#    active_risk = df_summary['Active Risk'].mean()
#    act_s_risk = df_summary['Active Specific Risk'].mean()
#    turnover = df_summary['Turnover'].mean()
#    turnover_max = df_summary['Turnover'].iloc[1:].max()
#    turnover_min = df_summary['Turnover'].iloc[1:].min()
#    
#    index_name = ['Total return','Annualized total return','Total Benchmark return','Annualized Benchmark return',\
#            'Total Risk(average)','Active Risk(average)','Active Specific Risk(average)','turnover(average)',\
#            'turnover(max)','turnover(min)']
#            
#    data = [t_ret, ret_Y, t_ret_benchmark, ret_benchmark, total_risk, active_risk, act_s_risk, turnover,turnover_max,turnover_min]
#    
#    df_info = pd.DataFrame(data,index =index_name, columns=['results'])
#    
#    
#    df_info = df_info * 100
#    df_info = df_info.round(2)
#    
#    df_info['results'] = df_info['results'].apply(lambda x: str(x) + '%')
