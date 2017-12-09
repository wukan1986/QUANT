# -*- coding: utf-8 -*-


import os
import pandas as pd
import time
from time import strftime, localtime
from tools import get_tradeDay, client_db
from sklearn import linear_model
import numpy as np


start = '20061201'
# start = '20110101'
end = strftime("%Y%m%d", localtime())
output_path = 'F:/factor_data/raw_data'

factor_name = 'PDPS' #'DEVIDEND'
if not os.path.exists(output_path+'/'+factor_name):
    os.mkdir(output_path+'/'+factor_name)

fre = 'month'
trade_day = get_tradeDay.wind(start, end, fre=fre)

getdb = client_db.read_db(type='wind')
getdb2 = client_db.read_db(type='ctquant2')

def get_timerange(today, sql):
    end = today
    start = str(int(today[:4]) - 3) + today[4:]
    sql1 = sql + " where (S_DIV_PRELANDATE >= %s) and (S_DIV_PRELANDATE <= %s)" % (start, end)
    df = getdb.db_query(sql1)
    return df

def get_timerange2(today, sql):
    end = today
    start = str(int(today[:4]) - 3) + today[4:]
    sql1 = sql + " where (S_STM_ACTUAL_ISSUINGDATE >= %s) and (S_STM_ACTUAL_ISSUINGDATE <= %s)" % (start, end)
    df = getdb.db_query(sql1)
    return df

def get_timerange3(today, sql):
    end = today
    start = str(int(today[:4]) - 3) + today[4:]
    sql1 = sql + " where (ANN_DT >= %s) and (ANN_DT <= %s)" % (start, end)
    df = getdb.db_query(sql1)
    return df

def db_clean(data):
    data.sort_values(['S_INFO_WINDCODE', 'S_DIV_PRELANDATE'], inplace=True)
    data.index = range(len(data))
    return data

def db_clean2(data):
    data.sort_values(['S_INFO_WINDCODE', 'REPORT_PERIOD', 'ANN_DT'], inplace=True)
    data.index= range(len(data))
    return data

def convert(x):
    if x[4:]<= '0331':
        xx = x[:4] + '0331'
    elif (x[4:]<= '0630') and (x[4:] > '0331'):
        xx = x[:4] + '0630'
    elif (x[4:]<= '0930') and (x[4:] > '0630'):
        xx = x[:4] + '0930'
    elif (x[4:]<= '1231') and (x[4:] > '0930'):
        xx = x[:4] + '1231'
    return xx


def filter_period(m):
    if m[4:] == '1231':
        return True
    else:
        return False

def get_model_data(df):
    data = df.copy()
    data['period'] = data['REPORT_PERIOD'].apply(lambda x: x[4:])
    data = data[data['period']=='0930']
    # 获取test数据
    data_test = data.groupby(['S_INFO_WINDCODE']).apply(lambda x: x.iloc[-1])
    data_test = data_test[['S_INFO_WINDCODE','REPORT_PERIOD']]
    # 获取train数据
    df_size = data.groupby(['S_INFO_WINDCODE']).size()
    df_size = df_size[df_size==1]
    exclude_code = df_size.index.tolist()
    data_train = data[~data['S_INFO_WINDCODE'].isin(exclude_code)]
    data_train = data_train.groupby(['S_INFO_WINDCODE']).apply(lambda x: x.iloc[-2])
    data_train['ann_report'] = data_train['REPORT_PERIOD'].apply(lambda x: x[:4]+'1231')

    data_train = data_train[['S_INFO_WINDCODE','REPORT_PERIOD','ann_report']]
    data_train.index = range(len(data_train))
    data_test.index = range(len(data_test))
    y_train = data_train[['S_INFO_WINDCODE','ann_report']]
    y_train.columns = ['S_INFO_WINDCODE','REPORT_PERIOD']
    x_train = data_train[['S_INFO_WINDCODE', 'REPORT_PERIOD']]

    y_train['key']=y_train['S_INFO_WINDCODE'] + y_train['REPORT_PERIOD']
    x_train['key'] = x_train['S_INFO_WINDCODE'] + x_train['REPORT_PERIOD']
    data_test['key'] = data_test['S_INFO_WINDCODE'] + data_test['REPORT_PERIOD']

    return x_train, y_train, data_test

# def filter_extreme_data(data,col_list):
#     for col in col_list:
#         up_side = data[col].mean() + 5*data[col].std()
#         down_side = data[col].mean() - 5*data[col].std()
#         data = data[(data[col]<=up_side)&(data[col]>=down_side)]
#     return data
def filter_extreme_data(data,col):
    up_side = data[col].mean() + 5*data[col].std()
    down_side = data[col].mean() - 5*data[col].std()
    data = data[(data[col]<=up_side)&(data[col]>=down_side)]
    return data

for i in range(len(trade_day)):
    today = trade_day[i]
    print(today)
    ##############################################
    # sql1 = "select S_INFO_WINDCODE,CASH_DVD_PER_SH_PRE_TAX*S_DIV_BASESHARE as CASH_DVD,S_DIV_PRELANDATE,REPORT_PERIOD from AshareDividend"
    sql1 = "select S_INFO_WINDCODE,CASH_DVD_PER_SH_PRE_TAX as CASH_DVD,S_DIV_BASESHARE,S_DIV_PRELANDATE,REPORT_PERIOD from AshareDividend"
    df = get_timerange(today, sql1)
    df = db_clean(df)

    ### 去掉分红不是年报给出来的 ！！！
    df['filter'] = df['REPORT_PERIOD'].apply(filter_period)
    df = df[df['filter']]
    del df['filter']
    df = df[['S_INFO_WINDCODE', 'REPORT_PERIOD', 'CASH_DVD']]
    df['key'] = df['S_INFO_WINDCODE'] + df['REPORT_PERIOD']

    #################################################
    sql2 = "select S_INFO_WINDCODE,REPORT_PERIOD,S_STM_ACTUAL_ISSUINGDATE from AShareIssuingDatePredict"
    df2 = get_timerange2(today, sql2)
    df2 = df2.sort_values(['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_STM_ACTUAL_ISSUINGDATE'])

    x_train, y_train, x_test = get_model_data(df2)


    y_train1 = pd.merge(df,y_train, on= 'key')

    y_train1 = y_train1[['S_INFO_WINDCODE_x','REPORT_PERIOD_x','CASH_DVD','key']]
    y_train1.columns = ['S_INFO_WINDCODE', 'REPORT_PERIOD', 'CASH_DVD','key']
    ###################################################
    sql3 = "select S_INFO_WINDCODE,ANN_DT,REPORT_PERIOD,S_FA_UNDISTRIBUTEDPS,S_FA_SURPLUSRESERVEPS,S_FA_ORPS,S_FA_CFPS from AShareFinancialIndicator"
    df3 = get_timerange3(today, sql3)
    df3 = db_clean2(df3)

    df3['key'] = df3['S_INFO_WINDCODE'] + df3['REPORT_PERIOD']

    # x_train1 = pd.merge(df3, x_train, on='key')
    # x_train1 = x_train1[['S_INFO_WINDCODE_x', 'REPORT_PERIOD_x', 'S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS', 'key']]
    # x_train1.columns = ['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS', 'key']
    #########################################################
    sql4 = "select S_INFO_WINDCODE,ANN_DT,S_HOLDER_ENDDATE,S_HOLDER_PCT from AShareInsideHolder"
    df4 = get_timerange3(today, sql4)
    df4.columns = ['S_INFO_WINDCODE','ANN_DT','REPORT_PERIOD','S_HOLDER_PCT']
    df4 = db_clean2(df4)
    df4 = df4[['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_HOLDER_PCT']]
    df4 = df4.groupby(['S_INFO_WINDCODE', 'REPORT_PERIOD']).agg(sum)
    df4 = df4.reset_index()
    df4['REPORT_PERIOD'] = df4['REPORT_PERIOD'].apply(convert)
    df4 = df4.groupby(['S_INFO_WINDCODE', 'REPORT_PERIOD']).last()
    df4 = df4.reset_index()

    df4 = df4.pivot(index='REPORT_PERIOD', columns='S_INFO_WINDCODE', values='S_HOLDER_PCT')
    df4 = df4.fillna(method='pad')

    df4 = df4.stack()
    df4 = df4.reset_index()
    df4.columns = ['REPORT_PERIOD','S_INFO_WINDCODE','S_HOLDER_PCT']
    df4.sort_values(['S_INFO_WINDCODE', 'REPORT_PERIOD'], inplace=True)
    df4.index = range(len(df4))
    df4['key'] = df4['S_INFO_WINDCODE'] + df4['REPORT_PERIOD']

    df4 = df4[['S_HOLDER_PCT','key']]

    ######################################################
    df5 = pd.merge(df3, df4, how='left',on='key')

    x_train1 = pd.merge(df5, x_train, on='key')
    x_train1 = x_train1[['S_INFO_WINDCODE_x', 'REPORT_PERIOD_x', 'S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS', 'S_HOLDER_PCT', 'key']]
    x_train1.columns = ['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS', 'S_HOLDER_PCT', 'key']
    x_train1['S_HOLDER_PCT'] = x_train1['S_HOLDER_PCT']/100.0
    ####
    x_test1 = pd.merge(df5, x_test, on='key')
    x_test1 = x_test1[['S_INFO_WINDCODE_x', 'REPORT_PERIOD_x', 'S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS', 'S_HOLDER_PCT', 'key']]
    x_test1.columns = ['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS', 'S_HOLDER_PCT', 'key']
    x_test1['S_HOLDER_PCT'] = x_test1['S_HOLDER_PCT']/100.0

    #########################################################
    data_train = pd.merge(x_train1,y_train1,on='S_INFO_WINDCODE')
    data_train1 = data_train[['CASH_DVD','S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS','S_HOLDER_PCT']]
    ## 去掉y=0的 x中有缺失值的
    data_train1 = data_train1[data_train1['CASH_DVD'] != 0]
    data_train1 = data_train1.dropna()·
    #################################################
    data_train1 = filter_extreme_data(data_train1,'CASH_DVD')

    #################################################
    x_train = data_train1[['S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS','S_HOLDER_PCT']].values
    y_train = data_train1['CASH_DVD'].values
    clf = linear_model.LinearRegression()
    model = clf.fit(x_train, y_train)

    print(model.score(x_train, y_train))
    #################################################################
    x_test1 = x_test1.dropna()
    data_test = x_test1[['S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS','S_HOLDER_PCT']]

    x_test = data_test[['S_FA_UNDISTRIBUTEDPS','S_FA_SURPLUSRESERVEPS','S_FA_ORPS','S_FA_CFPS','S_HOLDER_PCT']].values
    y_predict = model.predict(x_test)

    x_test1['PDPS_raw'] = y_predict

    df_predict = x_test1[['S_INFO_WINDCODE','PDPS_raw']].copy()
    df_predict.to_csv('%s/%s/%s_raw_CN_%s.csv' % (output_path,factor_name, factor_name, today), index=None)