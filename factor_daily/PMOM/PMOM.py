# -*- coding: utf-8 -*-
import OracleClient2 as client, Config
import ACalendar
import datetime as dt
import pandas as pd
import math
import FileUtility
import threading
from time import strftime, localtime


class pmom(object):
    def __init__(self, output_path):
        self.__quant_client = client.OracleClient(Config.get_db_conn('quant2'))
        self.__calendar = ACalendar.ACalendar()
        self.output_path = output_path

    def update(self, alpha_date, month, days, discard_days):

        end = self.__calendar.get_trading_date_before(alpha_date, discard_days)
        start = self.__calendar.get_trading_date_before(end, days - 1)
        between_prices = self.__get_price_between(start, end)
        suspend_statis = self.__get_suspend_statis(between_prices)
        start_prices = self.__get_price(start)
        end_prices = self.__get_price(end)
        code_info = self.__get_code_info()
        code_info = code_info[
            code_info['list_date'] < (dt.date(start.year - 1, start.month, start.day)).strftime('%Y%m%d')]
        code_info = code_info[code_info['delist_date'].isnull()]
        start_prices = start_prices.merge(code_info, on='code')[['code', 'close']]
        end_prices = end_prices.merge(code_info, on='code')[['code', 'close']]
        start_prices = start_prices.drop_duplicates()
        end_prices = end_prices.drop_duplicates()
        merge_prices = start_prices.merge(end_prices, on='code')
        merge_prices['holding_return'] = merge_prices['close_y'] / merge_prices['close_x'] - 1
        merge_prices = merge_prices.merge(suspend_statis, on='code', how='left').fillna(0)
        merge_prices['adj_return'] = merge_prices['holding_return'] * (
            (days / (days - merge_prices['suspend_count'])).apply((lambda a: math.sqrt(a))))
        result = merge_prices[merge_prices['suspend_count'] <= 0.75 * days]
        self.__save(result, alpha_date, month, discard_days)

    def __save(self, df, alpha_date, month, discard_days):
        df['code'] = df['code'] + '-CN'
        FileUtility.create_dir('{2}/PMOM{0}{1}'.format(month, discard_days, self.output_path))
        df = df[['code', 'adj_return']]
        df.columns = ['S_INFO_WINDCODE', 'PMOM%s%s_raw' % (month, discard_days)]
        df = df.to_csv(
            '{3}/PMOM{1}{2}/PMOM{1}{2}_raw_CN_{0}.csv'.format(alpha_date.strftime('%Y%m%d'), month,
                                                              discard_days, self.output_path), index=False)

    def __get_suspend_statis(self, between_prices):
        between_prices = between_prices[between_prices['status'] == '停牌']
        res = []
        for g in between_prices.groupby(['code']):
            res.append([g[0], g[1].shape[0]])
        return pd.DataFrame(data=res, columns=['code', 'suspend_count'])

    def __get_price_between(self, start, end):
        sql = "select TRADE_DT,substr(s_info_windcode,0,instr(s_info_windcode,'.')-1),S_DQ_TRADESTATUS from ASHAREEODPRICES where TRADE_DT>=:1 and TRADE_DT<=:2"
        params = (start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
        return self.__quant_client.get(sql, columns=['date', 'code', 'status'], params=params)

    def __get_price(self, date):
        sql = "select SYMBOL,LTDR from P_GG_KEYDATA where TDATE=:1 and exchange like 'CN%'"
        # sql = "select substr(s_info_windcode,0,instr(s_info_windcode,'.')-1),S_DQ_ADJCLOSE from ASHAREEODPRICES where TRADE_DT=:1"
        params = (int(date.strftime('%Y%m%d')),)
        return self.__quant_client.get(sql, columns=['code', 'close'], params=params)

    def __get_code_info(self):
        sql = "select substr(s_info_windcode,0,instr(s_info_windcode,'.')-1),S_INFO_LISTDATE,S_INFO_DELISTDATE,S_INFO_LISTBOARDNAME from ASHAREDESCRIPTION"
        return self.__quant_client.get(sql, columns=['code', 'list_date', 'delist_date', 'list_board'])

    def run(self):
        ls_th = []
        for year in range(2007, dt.date.today().year + 1):
            self.run_unit(year)
        print('end.')

    def run_unit(self, year):
        # vals = [(36, 720, 100)]
        vals = [(9, 185, 40), (6, 130, 40), (3, 65, 60), (6, 130, 60), (12, 240, 20)]
        month = 13 if year < dt.date.today().year else dt.date.today().month
        for m in range(1, month):
            alpha_dt = self.__calendar.get_last_trading_date_during(year, m)
            print(alpha_dt)
            for val in vals:
                print(val)
                self.update(dt.datetime.strptime(alpha_dt, '%Y%m%d').date(), val[0], val[1], val[2])


if __name__ == '__main__':
    ls_th =[]
    dt.datetime.strptime('20171101', '%Y%m%d').date()
    for year in range(2017, dt.date.today().year + 1):
        pm = PMOM()
        t = threading.Thread(target=pm.run_unit,args=(year,))
        ls_th.append(t)
    for t in ls_th:
        t.start()
    for t in ls_th:
        t.join()
    print('end.')


    # ls_th = []
    # dt.datetime.strptime('20171101', '%Y%m%d').date()
    # for year in range(2006, dt.date.today().year + 1):
    #     pm = PMOM()
    #     pm.run_unit(year)
    # print('end.')

    # 每日更新的
    #
    # output_path = 'F:/factor_data/raw_data'
    # output_path = 'Z:/daily_data/alpha'
    # pm = pmom()
    # vals = [(9, 185, 40), (6, 130, 40), (3, 65, 60), (6, 130, 60), (12, 240, 20)]
    # today = dt.datetime.strptime(strftime("%Y%m%d", localtime()), '%Y%m%d').date()
    # for val in vals:
    #     print(val)
    #     pm.update(today, val[0], val[1], val[2], output_path)
