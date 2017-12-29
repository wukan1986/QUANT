# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 09:34:52 2017

@author: shiyunchao
"""

import pandas as pd
import numpy as np
import os
from time import strftime, localtime, time
import sys
from sklearn import linear_model

sys.path.append("F:/QUANT/")
reload(sys)
from tools import get_tradeDay
from factor_daily import factor_fundmental, factor_fundmental1


class stdfcff(object):
    def __init__(self, today, raw_dirpath, new_dirpath, flag, fre):
        self.sql = "select WIND_CODE,ANN_DT,REPORT_PERIOD,s_fa_fcff from AShareFinancialIndicator"
        self.sql2 = "select WIND_CODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,tot_assets from AShareBalanceSheet"
        self.factor_filename1 = 'fcff'
        self.factor_filename2 = 'tot_asset'
        self.today = today

        self.raw_dirpath = raw_dirpath

        self.new_dirpath = new_dirpath  # 处理好的因子存放位置

        self.cal_model1 = 'ttm'
        self.cal_model2 = 'year_ave'
        self.flag = flag  # 0 回测 1更新
        self.fre = fre
        self.fre_shift_list = range(6)
        self.new_factor_name = 'fcftar'
        self.new_factor_name2 = 'stdfcf0'
        self.new_factor_name3 = 'stdfcf1'



    def get_net_income(self):
        for fre_shift in self.fre_shift_list:
            if self.new_factor_name2 == 'stdfcf0':
                fcff = factor_fundmental1.factorGet(self.sql, self.cal_model1, self.flag, self.factor_filename1,
                                                    self.raw_dirpath, fre_shift,
                                                    date=self.today, fre=self.fre)
                fcff.backtest_or_update()

            elif self.new_factor_name2 == 'stdni0':
                net_income = factor_fundmental.factorGet(self.sql, self.cal_model1, self.flag, self.factor_filename1,
                                                         self.raw_dirpath, fre_shift,
                                                         date=self.today, fre=self.fre)
                net_income.backtest_or_update()

    def get_tot_asset(self):
        for fre_shift in self.fre_shift_list:
            tot_asset = factor_fundmental.factorGet(self.sql2, self.cal_model2, self.flag, self.factor_filename2,
                                                    self.raw_dirpath, fre_shift,
                                                    date=self.today, fre=self.fre)
            tot_asset.backtest_or_update()

    def update(self, i, file1, factor_filename1, cal_model1, file2, factor_filename2, cal_model2, new_factor_path):
        path1 = file1 + '/' + factor_filename1 + '_' + cal_model1 + '_' + i + '.csv'
        path2 = file2 + '/' + factor_filename2 + '_' + cal_model2 + '_' + i + '.csv'

        df1 = pd.read_csv(path1, header=None)
        df2 = pd.read_csv(path2, header=None)
        df1.columns = ['code', 'factor1']
        df2.columns = ['code', 'factor2']

        df = pd.merge(df1, df2, on='code', how='outer')
        df = df.replace({'Na': np.nan, 'Ne': np.nan})
        df['factor1'] = df['factor1'].astype('float64')
        df['factor2'] = df['factor2'].astype('float64')
        df['factor'] = df['factor1'] / df['factor2']
        df = df[['code', 'factor']]

        df['code'] = df['code'].apply(lambda x: x.split('.')[0] + '-CN')

        df.to_csv('%s/%s_%s.csv' % (new_factor_path, self.new_factor_name, i), index=None, header=None)

    def get_fcftar(self):
        self.trade_day = get_tradeDay.wind('20080401', self.today, fre=self.fre)
        if not os.path.exists(self.raw_dirpath):
            os.mkdir(self.raw_dirpath)

        for fre in self.fre_shift_list:
            file1 = self.raw_dirpath + '/' + self.factor_filename1 + '_' + self.cal_model1 + '_' + str(fre)
            file2 = self.raw_dirpath + '/' + self.factor_filename2 + '_' + self.cal_model2 + '_' + str(fre)

            new_factor_path = self.raw_dirpath + '/' + self.new_factor_name + str(fre)
            if not os.path.exists(new_factor_path):
                os.mkdir(new_factor_path)
            if self.flag == 0:
                for i in self.trade_day:
                    self.update(i, file1, self.factor_filename1, self.cal_model1, file2, self.factor_filename2,
                                self.cal_model2, new_factor_path)
            elif self.flag == 1:
                self.update(self.today, file1, self.factor_filename1, self.cal_model1, file2, self.factor_filename2,
                            self.cal_model2, new_factor_path)

    def get_regression(self,df_total):
        clf = linear_model.LinearRegression()
        x = [[1], [2], [3], [4], [5]]
        coef = []
        r2 = []
        for _, row in df_total.iterrows():
            if len(row.dropna()) == 5:
                y = row.values
                model = clf.fit(x, y)
                coef.append(model.coef_[0])
                r2.append(model.score(x, y))
            else:
                coef.append(np.nan)
                r2.append(np.nan)
        df_regress = pd.DataFrame([])
        df_regress['code'] = df_total.index.tolist()
        df_regress['coef'] = coef
        df_regress['r2'] = r2
        return df_regress
    ######################
    def updatestd(self, ii, factor_num, new_dirpath, new_factor_name, new_factor_path2, new_factor_name2):
        df_total = pd.DataFrame([], columns=['code'])
        for num in factor_num:
            new_factor_path = new_dirpath + '/' + new_factor_name + str(num)

            path = new_factor_path + '/' + new_factor_name + '_' + str(ii) + '.csv'
            df = pd.read_csv(path, header=None)
            df.columns = ['code', 'factor' + str(num)]
            df_total = pd.merge(df, df_total, on='code', how='outer')
        df_total.set_index('code', inplace=True)

        if new_factor_name2 == 'stdfcf0':
            df_regress = self.get_regression(df_total)

            df_regress['sign_r2'] = np.sign(df_regress['coef']) * df_regress['r2']
            df_regress = df_regress[['code','sign_r2']]
            df_regress.columns = ['S_INFO_WINDCODE','FCFSR2_raw']
            df_regress.to_csv('%s/FCFSR2/FCFSR2_raw_CN_%s.csv'%(self.new_dirpath, ii), index=None)


        stdni = df_total.std(axis=1)
        stdni = stdni[stdni != 0]
        stdni = np.log(stdni)
        stdni = -stdni

        stdni = pd.DataFrame(stdni)
        stdni.reset_index(inplace=True)
        stdni.columns = ['S_INFO_WINDCODE', '%s_raw' % new_factor_name2.upper()]
        # stdni.to_csv('%s/%s_%s.csv' % (new_factor_path2.upper(), new_factor_name2.upper(), ii), index=None, header=None)
        stdni.to_csv('%s/%s_raw_CN_%s.csv' % (new_factor_path2.upper(), new_factor_name2.upper(), ii), index=None)

    def get_stdfcf0(self, new_factor_name2, factor_num):
        new_factor_path2 = self.new_dirpath + '/' + new_factor_name2

        if not os.path.exists(new_factor_path2):
            os.mkdir(new_factor_path2)

        if not os.path.exists(self.new_dirpath + '/' + 'FCFSR2'):
            os.mkdir(self.new_dirpath + '/' + 'FCFSR2')

        # factor_num = range(0, 5)

        if self.flag == 0:
            for ii in self.trade_day:
                self.updatestd(ii, factor_num, self.raw_dirpath, self.new_factor_name, new_factor_path2,
                               new_factor_name2)
        elif self.flag == 1:
            self.updatestd(self.today, factor_num, self.raw_dirpath, self.new_factor_name, new_factor_path2,
                           new_factor_name2)

    def run(self):
        self.get_net_income()
        self.get_tot_asset()
        self.get_fcftar()
        self.get_stdfcf0(self.new_factor_name2, range(0, 5))
        self.get_stdfcf0(self.new_factor_name3, range(1, 6))


if __name__ == '__main__':
    # flag = 1  # 0 回测 1更新
    # fre = 'day'
    #
    # today = strftime("%Y%m%d", localtime())  # '20170929'
    # raw_dirpath = 'F:/QUANT/factor_daily/fcff/raw_data'
    # new_dirpath = 'F:/factor_data/raw_data'
    #
    # srdfcf = stdfcff(today, raw_dirpath, new_dirpath, flag, fre)
    # srdfcf.run()

    flag = 0  # 0 回测 1更新
    fre = 'day'

    today = '20171204'  # '20170929'
    raw_dirpath = 'F:/factor_data/test_temp'
    new_dirpath = 'F:/factor_data/test_data'

    srdfcf = stdfcff(today, raw_dirpath, new_dirpath, flag, fre)
    srdfcf.run()

