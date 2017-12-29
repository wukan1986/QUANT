# -*- coding: utf-8 -*-import pandas as pd

import pandas as pd
from datetime import datetime, timedelta
import sys
from time import strftime, localtime

sys.path.append("F:/QUANT/")
reload(sys)
from tools.client_db import read_db
from tools import get_tradeDay


def get_new_stock(day, account_name, output_path, get_db):
    t = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    sql = "select s_info_code,s_info_listdate from AShareDescription where s_info_listdate>%s" % t
    newshares = get_db.db_query(sql)
    # newshares = newshares['S_INFO_CODE'].values.tolist()
    newshares = newshares[['S_INFO_CODE']]
    newshares['value'] = 1
    newshares['S_INFO_CODE'] = newshares['S_INFO_CODE'].apply(lambda x: x + '-CN')

    df = pd.DataFrame([['', 'GROUP'], ['name', account_name], ['unit', 'number'], ['', '']],
                      columns=['S_INFO_CODE', 'value'])
    df = pd.concat([df, newshares])

    df.to_csv('%s/%s' % (output_path, 'no_sell_99991231.csv'), index=None, header=None)


def run(start, end, today, account_name, dirpath, output_path, flag, fre):
    getdb = read_db(type='wind')
    tradeday = get_tradeDay.wind(start, end, fre=fre)
    tradeday = tradeday.iloc[:-1]
    if flag == 0:
        for day in tradeday:
            print(day)
            get_new_stock(day, account_name, output_path, getdb)
    else:
        get_new_stock(today, account_name, output_path, getdb)


if __name__ == '__main__':
    print('new_share start')
    start = '20171130'
    end = strftime("%Y%m%d", localtime())
    today = strftime("%Y%m%d", localtime())
    dirpath = 'G:/ESG100Enhanced/FA'
    output_path = 'Z:/daily_data/no_sell'
    fre = 'day'
    account_name = 'no_sell_ESG100-pdt'
    flag = 1
    run(start, end, today, account_name, dirpath, output_path, flag, fre)
    print('Done!')
