# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, timedelta
import sys
import yaml
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

    # df = pd.DataFrame([['', 'GROUP'], ['name', account_name], ['unit', 'number'], ['', '']],
    #                   columns=['S_INFO_CODE', 'value'])
    # df = pd.concat([df, newshares])

    newshares.to_csv('%s/%s' % (output_path, 'nosell_%s_99991231.csv'%account_name), index=None, header=None)


def run(start, end, today, output_path, flag, fre):
    config_path = 'Z:/daily_data/config/config.yaml'
    with open(config_path) as f:
        config = yaml.load(f.read())
    account = config['account']
    for key, sub_account in account.items():
        if sub_account['type'] == 'p':
            account_name = sub_account['subaccount'].split('account-')[1]
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
    output_path = 'Z:/daily_data/no_sell'
    fre = 'day'
    flag = 1
    run(start, end, today, output_path, flag, fre)
    print('Done!')
