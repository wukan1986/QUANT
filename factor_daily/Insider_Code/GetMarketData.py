# -*- coding: UTF-8 -*-

import cx_Oracle
import uqer
from uqer import DataAPI
import pandas as pd
import sys
import numpy as np
import GetOracle as orl
import GetTradeDay as td

def GetAllStocks(type):
    sql = 'select s_info_windcode,s_info_compnameeng as NAME,s_info_listdate as LISTDATE,s_info_delistdate as DELISTDATE from AShareDescription'
    data = orl.loaddata(type, sql)
    temp = ['A' in i for i in list(data['S_INFO_WINDCODE'])]
    data['REMARK'] = pd.Series(temp, index = data.index)
    A = data[data['REMARK'] == False]
    #A['LOCID'] = 'CN'+ A['LOCID']
    A = A.drop('REMARK', axis=1)
    return A

def GetStockCap_byDate(StockID, tdate):
    sql = "select distinct change_dt from AShareCapitalization where (s_info_windcode='%s') and (change_dt<='%s') order by change_dt"%(StockID, tdate)
    data = orl.loaddata('wind', sql)
    latest_date = data.ix[len(data)-1, 'CHANGE_DT']
    sql = "select s_info_windcode,change_dt,tot_shr,float_shr,float_a_shr from AShareCapitalization where (s_info_windcode='%s') and (change_dt='%s') order by change_dt"%(
        StockID, latest_date)
    data = orl.loaddata('wind', sql)
    cap = pd.Series()
    cap['TotalShare'] = data.ix[0, 'TOT_SHR'] * 10000
    cap['FloatShare'] = data.ix[0, 'FLOAT_SHR'] * 10000
    cap['FloatAShare'] = data.ix[0, 'FLOAT_A_SHR'] *10000
    return cap

def StockZF(Data):
    for i in range(len(Data)):
        if i==0:
            Data.at[i, 'RATE'] = None
        else:
            Data.at[i, 'RATE'] = Data.ix[i, 'CLOSE']/Data.ix[i-1, 'CLOSE']-1
    Data = Data.dropna(axis=0, how='any')
    Data = Data.reset_index(drop=True)
    return Data

def StockClosePrice(StockID, tdate):
    sql = "select trade_dt,s_info_windcode,s_dq_close from AShareEODPrices where (S_INFO_WINDCODE='%s') and (trade_dt='%s')"% (
              StockID, tdate)
    res = orl.loaddata('wind', sql)
    if len(res) > 0: return res.ix[0, 'S_DQ_CLOSE']
    else: return None

def StockAvgClosePrice(StockID, begt, endt):
    sql = "select trade_dt,s_info_windcode,s_dq_close from AShareEODPrices where (S_INFO_WINDCODE='%s') and (trade_dt>='%s') and (trade_dt<='%s')"% (
              StockID, begt, endt)
    res = orl.loaddata('wind', sql)
    if len(res) > 0: return round(res['S_DQ_CLOSE'].mean(),2)
    else: return None

def GetAllStockSWInd_byDate(tdate):
    sql = "select s_info_windcode,sw_ind_code,entry_dt,remove_dt from AShareSWIndustriesClass where (entry_dt<='%s')"%(tdate)
    data = orl.loaddata('wind', sql)
    data = data.fillna('21001231')
    data.columns = ['S_INFO_WINDCODE', 'INDCODE', 'ENTRY', 'REMOVE']
    data = data[data.REMOVE >= tdate]
    data = data.reset_index(drop=True)
    return data

def GetAllStocksCap_byDate(tdate):
    sql = "select s_info_windcode,change_dt,tot_shr,float_a_shr from AShareCapitalization where (change_dt<='%s') order by change_dt"%(tdate)
    data = orl.loaddata('wind', sql)
    data.columns = ['S_INFO_WINDCODE', 'DATE', 'TOTALSHARE', 'FLOATSHARE']
    chgdate = data['DATE'].groupby(data['S_INFO_WINDCODE']).max()
    cap = pd.DataFrame()
    cap['S_INFO_WINDCODE'] = pd.Series(list(chgdate.index))
    cap['DATE'] = pd.Series(list(chgdate))
    cap = pd.merge(cap, data, how='left', on=['S_INFO_WINDCODE', 'DATE'])
    cap['TOTALSHARE'] = cap['TOTALSHARE'] * 10000
    cap['FLOATSHARE'] = cap['FLOATSHARE'] * 10000
    return cap

def GetAllStocks_byDate(tdate):
    sql = "select a.s_info_windcode,a.trade_dt,a.s_val_mv,a.s_dq_mv,a.tot_shr_today,a.float_a_shr_today," \
          "a.s_dq_close_today,b.s_info_listdate,b.s_info_delistdate" \
          " from AShareEODDerivativeIndicator a left join AShareDescription b on a.s_info_windcode=b.s_info_windcode " \
          "where (a.trade_dt = '%s') order by a.s_info_windcode"%(tdate)
    data = orl.loaddata('wind', sql)
    data.columns = ['S_INFO_WINDCODE', 'DATE', 'TOTMV', 'FLOATMV', 'TOTSHR', 'FLOATSHR', 'CLOSE', 'LISTDATE', 'DELISTDATE']
    return data

if __name__ =='__main__':
    begt = '20130101'
    endt = '20131130'
    StockID = '000001.SZ'
    # res = GetStockCap_byDate(StockID, endt)
    # res = StockClosePrice(StockID, endt)
    # res = StockAvgClosePrice(StockID, begt, endt)
    # res = GetAllStockSWInd_byDate(endt)
    # res = GetAllStocksCap_byDate(endt)
    res = GetAllStocks_byDate(endt)
    # res = GetClosePrice_ByDates(StockID, td.tdays(begt, endt, 'M'), 'wind')
    path = 'D:/work/Factors/TK/test.csv'
    res.to_csv(path, index=None)
    print(res)