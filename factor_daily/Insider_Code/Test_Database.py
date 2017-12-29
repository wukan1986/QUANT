import cx_Oracle
import pandas as pd
import numpy as np
import math
import sys
import GetOracle as orl
import GetTradeDay as td
import GetMarketData as md
import Insider
import datetime
import time
import schedule

def testInsiderTrade(StockID):
    sql = "select * from AShareInsiderTrade where (s_info_windcode='%s')"%(StockID)
    data = orl.loaddata('wind', sql)
    return data

def testMjrHolder(StockID):
    sql = "select * from AShareMjrHolderTrade where (s_info_windcode='%s') order by ann_dt" % (StockID)
    # sql = "select count(*) from AShareMjrHolderTrade"
    data = orl.loaddata('wind', sql)
    return data

def testBalanceSheet(StockID, tdate):
    # sql = "select * from AShareBalanceSheet where s_info_windcode ='%s' and REPORT_PERIOD = '%s' and STATEMENT_TYPE='408001000'" %(StockID, tdate)
    sql = "select * from AShareBalanceSheet where STATEMENT_TYPE='408001000'"
    data = orl.loaddata('wind', sql)

    return data

def testFloatHolder(StockID):
    sql = "select s_info_windcode,ann_dt,s_holder_enddate,s_holder_holdercategory,s_holder_name,s_holder_quantity from AShareFloatHolder where (s_info_windcode='%s') order by s_holder_enddate,s_holder_quantity desc" % (StockID)
    data = orl.loaddata('wind', sql)
    return data

def testInsHolder(StockID):
    sql = "select s_info_windcode,report_period,ann_date,s_holder_holdercategory,s_holder_name,s_holder_quantity,s_holder_pct from AShareinstHolderDerData where (s_info_windcode='%s') order by REPORT_PERIOD,s_holder_quantity desc" % (StockID)
    insdata = orl.loaddata('wind', sql)
    return insdata

def testShareData(tdate):
    sql = "select s_info_windcode,change_dt,tot_shr,float_a_shr from AShareCapitalization where (change_dt<='%s') order by change_dt" % (
    tdate)
    share = orl.loaddata('wind', sql)
    share.columns = ['CODE', 'DATE', 'TOTALSHARE', 'FLOATSHARE']
    return share



StockID = '000002.SZ'
BegT ='20070131'
EndT = '20080131'
tdate = '20170831'

res = testInsiderTrade(StockID)
# res = testMjrHolder(StockID)
path = 'D:/work/Factors/Insider/insiderdata.csv'
res.to_csv(path, index=None)
print(res)






