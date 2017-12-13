# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 11:30:50 2017

@author: shiyunchao
"""

import pandas as pd
from time import strftime, localtime, time
import shutil
import os
import yaml
from tools import get_tradeDay, client_db

from xutils import (Date,
                    Calendar,
                    Period)

cal = Calendar('China.SSE')

today = strftime("%Y%m%d", localtime())
today = Date.strptime(today, '%Y%m%d')

pre_day = cal.advanceDate(today, Period('-2b'))
pre_day = pre_day.strftime("%Y%m%d")

today = cal.advanceDate(today, Period('-1b'))
today = today.strftime("%Y%m%d")

pre_day = '20171206'
today = '20171207'

'''
注意 这里的today就是指前一个交易日！！！  因为数据读取 保存日期都不会出现当天日期 
'''

with open('Z:/daily_data/config/config.yaml') as f:
    config = yaml.load(f.read())


### 计算昨日收益
def get_stock_ret(config, today):
    ret_path = config['daily_ret_path']
    df_ret = pd.read_csv('%s/CNE5_LOCALID_%s.DRET' % (ret_path, today[2:]), skiprows=1)
    df_ret = df_ret[[u'BARRID', u'LOCID', u'RETURN%']]

    df_temp = pd.read_csv('%s/CNE5S_LOCALID_%s.RSK' % (ret_path, today[2:]), skiprows=1)
    df_temp = df_temp[df_temp['ESTU'] == 1]
    df_temp = df_temp[[u'BARRID', u'LOCID', u'ESTU']]

    df_total = pd.merge(df_temp, df_ret, on='BARRID')
    df_ret = df_total[['LOCID_x', 'RETURN%']]
    df_ret.columns = ['Symbol', 'ret']
    df_ret['Symbol'] = df_ret['Symbol'].apply(lambda x: x[2:] + '-CN')
    return df_ret


def get_index(start, end, benchmark):
    getdb = client_db.read_db(type='wind')
    sql = "select TRADE_DT,S_DQ_CLOSE from AIndexEODPrices where (TRADE_DT>='%s') and (TRADE_DT<='%s') and (S_INFO_WINDCODE = '%s')" % (
        start, end, benchmark)
    df_index = getdb.db_query(sql)
    df_index.columns = ['trade_day', 'close']
    return df_index


####  创建账户的初始holding 都是cash
"""
下面的改成循环！
52行 加 continue  新账号在第二天早上运行的时候就创建初始现金持仓 下面步骤就不执行   第二天axioma软件运行后生成新账户report 
"""
# sub_account = account['ZZ500']
account = config['account']
dirpath = config['dirpath']
df_ret = get_stock_ret(config, today)
account_type = {'s': 'simulation', 'p': 'production'}
for key, sub_account in account.items():
    holding_path = '%s/holding/%s/%s' % (dirpath, account_type[sub_account['type']], sub_account['subaccount'])
    if not os.path.exists(holding_path):
        os.makedirs(holding_path)
        init_holding = pd.DataFrame([['CSH_CNY', eval(sub_account['initial_asset'])]])
        init_holding.to_csv('%s/holding_%s.csv' % (holding_path, today), index=None, header=None)
        continue

    df = pd.read_csv('%s/report/%s/simulate__%s__Final Portfolio Asset Details - Account.csv' % (
        dirpath, account_type[sub_account['type']], sub_account['rebalance_name']), encoding='gb18030')
    df = df.iloc[:-6]
    df = df.iloc[:, [0, 3, 4]]
    df.columns = ['Symbol', 'Price', 'Shares']
    df1 = df[['Symbol', 'Shares']]
    df1.to_csv('%s/holding/%s/%s/holding_%s.csv' % (
        dirpath, account_type[sub_account['type']], sub_account['subaccount'], today), index=None)

    ### 注意 下面合并后 默认是组合里的股票都能在barra收益率里面找到
    df_p = pd.merge(df, df_ret, on='Symbol')
    df_p['ret'] = df_p['ret'] / 100.0
    ff = len(df_p) == len(df)
    if not ff:
        print('!!!!!!!!')
        print(u'%s 组合里的股票不能在barra收益率里面找到'%sub_account['subaccount'])

    portfolio_ret = (df_p['Price'] * df_p['Shares'] * df_p['ret']).sum() / (df_p['Price'] * df_p['Shares']).sum()

    ###计算基准收益率
    benchmark = sub_account['benchmark']
    df_index = get_index(pre_day, today, benchmark)
    df_index = df_index.set_index('trade_day')
    df_index_ret = df_index.loc[pre_day] / df_index.loc[today]
    df_index_ret = df_index_ret.values[0] - 1

    active_ret = portfolio_ret - df_index_ret

    # 现在summary路径和holding的路径是一样的
    summary_path = '%s/holding/%s/%s' % (dirpath, account_type[sub_account['type']], sub_account['subaccount'])
    if os.path.exists('%s/summary.csv' % summary_path):
        df_summary = pd.read_csv('%s/summary.csv' % summary_path, index_col=0)
        df_summary.loc[int(today)] = [portfolio_ret, df_index_ret, active_ret]
    else:
        df_summary = pd.DataFrame([[portfolio_ret, df_index_ret, active_ret]],
                                  columns=['portfolio_ret', 'benchmark_ret', 'active_ret'], index=[int(today)])
    df_summary.to_csv('%s/summary.csv' % summary_path)

##########################################################################################################

### 备份数据
'''
这边要注意的是 每天生成report是没有问题的， 但是如果有一天没有生成新的report 想要提醒用shutil.move  否则用shutil.copyfile
'''
def backup_file(old_path, new_path, today):
    filename = os.listdir(old_path)
    if len(filename) != 0:
        if not os.path.exists(new_path + '/' + today):
            os.mkdir(new_path + '/' + today)
        for i in filename:
            shutil.copyfile(old_path + '/' + i, new_path + '/' + today + '/' + i)
            # shutil.move(old_path + '/' + i, new_path + '/' + today + '/' + i)
        return 1
    else:
        print('!!!!!!!!!!')
        print(old_path)
        print(u'前一天没有优化持仓，请重新生成report，重新运行程序')
        return 0


old_path1 = '%s/report/simulation' % (dirpath)
new_path1 = '%s/report/simulation_history' % (dirpath)
backup_file(old_path1, new_path1, today)

'''
因为目前没有production账户 所以下面的注释掉  有了production账户 目前的程序肯定还需要根据实际情况修改
'''
old_path2 = '%s/report/production' % (dirpath)
new_path2 = '%s/report/production_history' % (dirpath)
backup_file(old_path2, new_path2, today)
