# -*- coding: utf-8 -*-

import pandas as pd
from dbfread import DBF
from datetime import datetime, date, timedelta
import sys
from time import strftime, localtime
import warnings
warnings.filterwarnings("ignore")
sys.path.append("F:/QUANT/")
reload(sys)
from tools.client_db import read_db
from tools import get_tradeDay



def get_holding(day, dirpath, output_path, get_db):
    table = DBF('%s/GZ000042%s.dbf'%(dirpath,day), load=True, encoding='iso-8859-1')
    df = pd.DataFrame(list(table))
    df['A1'] = df['A1'].apply(lambda x: x.encode('iso-8859-1').decode('gbk'))
    df['A0'] = df['A0'].apply(lambda x: x.encode('iso-8859-1').decode('gbk'))
    df['Type'] = df['A0'].str.slice(start=0, stop=8)
    df['Ticker'] = df['A0'].str.slice(start=8, stop=14)
    df0 = df[(df['Type'].isin(['11020201', '11020101', '11023201', '11023101', '11024201', '11024101'])) & (df['Ticker'] != '')]
    df0['A2'] = df0['A2'].map(int)
    df1 = df[df['A0'].isin(['1002', '3003'])]
    df2 = df[df['A0'].isin(['2203', '2204', '2206', '2207', '2209', '2501'])]
    df3 = df[df['A0'].isin(['1021', '1031', '1204', '1207'])]
    df1['A7'] = df1['A7'].apply(float)
    df2['A7'] = df2['A7'].apply(float)
    df3['A7'] = df3['A7'].apply(float)
    depositplusclearing = df1['A7'].sum()
    payables = df2['A7'].sum()
    others = df3['A7'].sum()
    holdings = df0[['Ticker', 'A2']].sort_values(by='Ticker')
    holdings.index = range(len(holdings))
    t = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

    # sql = "select s_info_code,s_info_listdate from AShareDescription where s_info_listdate>%s" % t
    # newshares = get_db.db_query(sql)
    # newshares = newshares['S_INFO_CODE'].values.tolist()
    # holdings = holdings[~holdings['Ticker'].isin(newshares)]
    holdings.index = range(len(holdings))
    holdings['Ticker'] = holdings['Ticker'].apply(lambda x: x + '-CN')
    holdings = holdings.append(
        [{'Ticker': 'CSH_CNY', 'A2': depositplusclearing - payables}, {'Ticker': 'Accrual', 'A2': others}],
        ignore_index=True)

    holdings.columns = ['Symbol', 'Shares']
    holdings.to_csv('%s/holding_%s.csv'%(output_path, day),index=None)



def run(start, end, today, dirpath, output_path,flag, fre):
    getdb = read_db(type='wind')
    tradeday = get_tradeDay.wind(start, end, fre=fre)
    tradeday = tradeday.iloc[:-1]
    # if not os.path.exists(dirpath):
    #    os.mkdir(dirpath)
    if flag==0:
        for day in tradeday:
           print(day)
           get_holding(day,dirpath, output_path,getdb)
    else:
        get_holding(today, dirpath, output_path,getdb)


if __name__ == '__main__':
    start = '20171130'
    end = strftime("%Y%m%d", localtime())
    today = strftime("%Y%m%d", localtime())
    dirpath = 'G:/ESG100Enhanced/FA'
    output_path = 'Z:/daily_data/holding/production/account-ESG100-pdt'
    fre = 'day'
    flag = 1
    run(start, end, today, dirpath, output_path,flag, fre)