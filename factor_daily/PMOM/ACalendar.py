# -*- coding: utf-8 -*-

import datetime as dt

import OracleClient as client
from Infrastructure import Config


class ACalendar:

    def __init__(self):
        self.__quant_client = client.OracleClient(Config.get_db_conn('quant2'))

    def get_last_trading_date(self,date):
        sql = "select max(TRADE_DAYS) from ASHARECALENDAR where TRADE_DAYS<'{0}'".format(date.strftime('%Y%m%d'))
        last = self.__quant_client.get_one(sql)
        return dt.datetime.strptime(last,'%Y%m%d').date()

    def get_trading_date_before(self,date,span):
        sql = 'select distinct(TRADE_DAYS) from ASHARECALENDAR where TRADE_DAYS < :1 and TRADE_DAYS>:2 order by TRADE_DAYS desc'
        params = (date.strftime('%Y%m%d'),(date-dt.timedelta(days=span*2+10)).strftime('%Y%m%d'))
        val = self.__quant_client.get(sql,params=params,columns=['TRADE_DAYS'])
        d_str = val['TRADE_DAYS'][span-1]
        return dt.datetime.strptime(d_str,'%Y%m%d').date()

    def get_trading_dates_between(self,start,end):
        sql = 'select distinct(TRADE_DAYS) from ASHARECALENDAR where TRADE_DAYS>=:1 and TRADE_DAYS<=:2 order by TRADE_DAYS asc'
        params = (start.strftime('%Y%m%d'),end.strftime('%Y%m%d'))
        return self.__quant_client.get(sql,params=params,columns=['TRADE_DAYS'])

    def get_trading_date_after(self,current,span):
        sql = 'select distinct(TRADE_DAYS) from ASHARECALENDAR where TRADE_DAYS > :1 and TRADE_DAYS<:2 order by TRADE_DAYS asc'
        params = (current.strftime('%Y%m%d'), (current + dt.timedelta(days=span * 2 + 10)).strftime('%Y%m%d'))
        val = self.__quant_client.get(sql, params=params, columns=['TRADE_DAYS'])
        d_str = val['TRADE_DAYS'][span - 1]
        return dt.datetime.strptime(d_str, '%Y%m%d').date()

    def get_last_trading_date_during(self,year,month):
        sql = 'select max(trade_days) from ASHARECALENDAR where trade_days<:1'
        if month == 12:
            params = (dt.date(year+1,1,1).strftime('%Y%m%d'),)
        else:
            params = (dt.date(year,month+1,1).strftime('%Y%m%d'),)
        val = self.__quant_client.get_one(sql,params)
        return val

if __name__=='__main__':
    ac = ACalendar()
    tds = ac.get_last_trading_date_during(2017,12)
    print(tds)


