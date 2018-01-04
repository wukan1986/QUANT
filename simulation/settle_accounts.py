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
import client_db
import get_market
from xutils import (Date,
                    Calendar,
                    Period)

class settle_account():
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.load(f.read())
        self.account = self.config['account']
        self.dirpath = self.config['dirpath']
        self.account_type = {'s': 'simulation', 'p': 'production'}

        '''
        注意 这里的today就是指前一个交易日！！！  因为数据读取 保存日期都不会出现当天日期 
        '''
        cal = Calendar('China.SSE')

        self.today = strftime("%Y%m%d", localtime())
        today = Date.strptime(self.today, '%Y%m%d')

        pre_day = cal.advanceDate(today, Period('-1b'))
        self.pre_day = pre_day.strftime("%Y%m%d")

        # self.pre_day = '20171227'
        # self.today = '20171228'


    ### 计算昨日收益
    def get_stock_ret(self, today):
        ret_path = self.config['daily_ret_path']
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

    def get_stock_ret2(self, today):
        field = ['S_INFO_WINDCODE', 'S_DQ_PCTCHANGE']
        df = get_market.wind_day(field, ref_date=today)
        df.columns = ['Symbol', 'ret']
        # df['ret'] = df['ret']/100.0
        df['Symbol'] = df['Symbol'].apply(lambda x: x.split('.')[0] + '-CN')
        return df

    def get_stock_close(self, day):
        field = ['S_INFO_WINDCODE', 'S_DQ_CLOSE']
        df = get_market.wind_day(field, ref_date=day)
        df.columns = ['Symbol', 'close']
        df['Symbol'] = df['Symbol'].apply(lambda x: x.split('.')[0] + '-CN')
        return df


    def get_index(self, start, end, benchmark):
        getdb = client_db.read_db(type='wind')
        sql = "select TRADE_DT,S_DQ_CLOSE from AIndexEODPrices where (TRADE_DT>='%s') and (TRADE_DT<='%s') and (S_INFO_WINDCODE = '%s')" % (
            start, end, benchmark)
        df_index = getdb.db_query(sql)
        df_index.columns = ['trade_day', 'close']
        df_index = df_index.set_index('trade_day')
        df_index_ret = df_index.loc[end] / df_index.loc[start]
        df_index_ret = df_index_ret.values[0] - 1
        return df_index_ret

    def get_index2(self, today, benchmark):
        from WindPy import *
        w.start()
        df_index_ret = w.wsd(benchmark, "pct_chg", today, today, "")
        return df_index_ret.Data[0][0] / 100.0

    def run(self):
        ####  创建账户的初始holding 都是cash
        """
        下面的改成循环！
        52行 加 continue  新账号在第二天早上运行的时候就创建初始现金持仓 下面步骤就不执行   第二天axioma软件运行后生成新账户report
        """
        # sub_account = account['ZZ500']

        df_ret = self.get_stock_ret2(self.today)

        for key, sub_account in self.account.items():
            holding_path = '%s/holding/%s/%s' % (self.dirpath, self.account_type[sub_account['type']], sub_account['subaccount'])
            if not os.path.exists(holding_path):
                os.makedirs(holding_path)
                init_holding = pd.DataFrame([['CSH_CNY', eval(str(sub_account['initial_asset']))]])
                init_holding.to_csv('%s/holding_%s.csv' % (holding_path, self.today), index=None, header=None)
                continue
            if sub_account['type'] == 's':
                df = pd.read_csv('%s/report/%s/%s__%s__Final Portfolio Asset Details - Account.csv' % (
                    self.dirpath, self.account_type[sub_account['type']], self.account_type[sub_account['type']],sub_account['rebalance_name']), encoding='gb18030')
                df = df.iloc[:-6]
                df = df.iloc[:, [0, 3, 4]]
                df.columns = ['Symbol', 'Price', 'Shares']
                df1 = df[['Symbol', 'Shares']]
                df1.to_csv('%s/holding/%s/%s/holding_%s.csv' % (
                    self.dirpath, self.account_type[sub_account['type']], sub_account['subaccount'], self.today),
                           index=None)
                df2 = df[['Symbol', 'Price', 'Shares']]
                df2.to_csv('%s/holding/%s/%s/holding_close_%s.csv' % (
                    self.dirpath, self.account_type[sub_account['type']], sub_account['subaccount'], self.today),
                           index=None)

            elif sub_account['type'] == 'p':
                df_tmp = pd.read_csv('%s/holding_%s.csv' % (holding_path, self.pre_day))
                df_tmp.columns = ['Symbol', 'Shares']
                df_close = self.get_stock_close(self.pre_day)
                df_close.columns = ['Symbol', 'Price']
                df2 = pd.merge(df_tmp, df_close, on='Symbol')
                df2.to_csv('%s/holding_close_%s.csv' % (holding_path, self.pre_day), index=None)
                ### 上面作用是为了下面算summary
                df_tmp = pd.read_csv('%s/holding_%s.csv' % (holding_path, self.today))
                df_tmp.columns = ['Symbol', 'Shares']
                df_close = self.get_stock_close(self.today)
                df_close.columns = ['Symbol', 'Price']

                try:
                    cash = df_tmp.loc[df_tmp.Symbol == 'CSH_CNY', 'Shares'].values[0] + df_tmp.loc[df_tmp.Symbol == 'Accrual', 'Shares'].values[0]
                except:
                    cash = 0
                df_nav = pd.merge(df_tmp, df_close, on='Symbol')
                nav = (df_nav['Shares'] * df_nav['Price']).sum() + cash
                df_nav = pd.DataFrame([nav])
                df_nav.to_csv('%s/nav/%s_NAV.csv'%(self.dirpath,sub_account['subaccount'].split('account-')[1]),index=None,header=None)


            ### 注意 下面合并后 默认是组合里的股票都能在barra收益率里面找到
            try:
                df11 = pd.read_csv('%s/holding_close_%s.csv' % (holding_path, self.pre_day))
            except:
                continue
            df_p = pd.merge(df11, df_ret, on='Symbol')
            df_p['ret'] = df_p['ret'] / 100.0

            ### 下面注释掉的 模拟账户是可以的 生产账户应该是==2 不过一般也不看的 ，所以如果要加上就判断一下账户类型 ==1 还是2
            '''
            ff = (len(df11) - len(df_p)) == 1
            if not ff:
                print('!!!!!!!!')
                print(u'%s 组合里的股票不能在barra收益率里面找到'%sub_account['subaccount'])
            '''
            portfolio_ret = (df_p['Price'] * df_p['Shares'] * df_p['ret']).sum() / (df_p['Price'] * df_p['Shares']).sum()

            ###计算基准收益率
            benchmark = sub_account['benchmark']

            df_index_ret = self.get_index2(self.today, benchmark)
            '''
            df_index_ret = self.get_index(self.pre_day, self.today, '000300.SH')
            用指数全收益代替
            '''
            active_ret = portfolio_ret - df_index_ret

            # 现在summary路径和holding的路径是一样的
            summary_path = '%s/holding/%s/%s' % (self.dirpath, self.account_type[sub_account['type']], sub_account['subaccount'])
            if os.path.exists('%s/summary.csv' % summary_path):
                df_summary = pd.read_csv('%s/summary.csv' % summary_path, index_col=0)
                df_summary.loc[int(self.today)] = [portfolio_ret, df_index_ret, active_ret]
            else:
                df_summary = pd.DataFrame([[portfolio_ret, df_index_ret, active_ret]],
                                          columns=['portfolio_ret', 'benchmark_ret', 'active_ret'], index=[int(self.today)])
            df_summary.to_csv('%s/summary.csv' % summary_path)


    ### 备份数据
    '''
    这边要注意的是 每天生成report是没有问题的， 但是如果有一天没有生成新的report 想要提醒用shutil.move  否则用shutil.copyfile
    '''
    def backup_file(self, old_path, new_path, today):
        filename = os.listdir(old_path)
        if len(filename) != 0:
            if not os.path.exists(new_path + '/' + today):
                os.mkdir(new_path + '/' + today)
            for i in filename:
                shutil.copyfile(old_path + '/' + i, new_path + '/' + today + '/' + i)
                # shutil.move(old_path + '/' + i, new_path + '/' + today + '/' + i)
        else:
            print('!!!!!!!!!!')
            print(old_path)
            print(u'前一天没有优化持仓，请重新生成report，重新运行程序')

    def run_backup(self):
        old_path1 = '%s/report/simulation' % (self.dirpath)
        new_path1 = '%s/report/simulation_history' % (self.dirpath)
        self.backup_file(old_path1, new_path1, self.today)

        '''
        因为目前没有production账户 所以下面的注释掉  有了production账户 目前的程序肯定还需要根据实际情况修改
        '''
        old_path2 = '%s/report/production' % (self.dirpath)
        new_path2 = '%s/report/production_history' % (self.dirpath)
        self.backup_file(old_path2, new_path2, self.today)

if __name__ == '__main__':
    config_path = 'Z:/daily_data/config/config.yaml'
    settle = settle_account(config_path)
    settle.run()
    settle.run_backup()