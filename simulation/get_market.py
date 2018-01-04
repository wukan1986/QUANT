# -*- coding: utf-8 -*-

# from ct_manager.api import get_equity_market, get_equity_market_eod, universe
import client_db


def wind_day(field, ref_date):
    field = ','.join(field)
    sql = "select %s from ASHAREEODPRICES where TRADE_DT='%s'" % (field, ref_date)
    db = client_db.read_db(type='wind')
    df = db.db_query(sql)
    return df


def wind(field, start_date, end_date):
    field = ','.join(field)
    sql = "select %s from ASHAREEODPRICES where TRADE_DT>='%s' and TRADE_DT<='%s'" % (field, start_date, end_date)
    db = client_db.read_db(type='wind')
    df = db.db_query(sql)
    return df


def postgresql_day(fields, ref_date, ticker=None):
    if ticker == None:
        df = get_equity_market_eod(ticker=['ashare'], fields=fields, ref_date=ref_date)
    else:
        df = get_equity_market_eod(ticker=ticker, fields=fields, ref_date=ref_date)
    return


def postgresql(fields, start_date, end_date, ticker=None):
    if ticker == None:
        df = get_equity_market(ticker=['ashare'], fields=fields, start_date=start_date, end_date=end_date)
    else:
        df = get_equity_market(ticker=ticker, fields=fields, start_date=start_date, end_date=end_date)
    return df


if __name__ == '__main__':
    import time

    t1 = time.time()

    # field = ['close', 'adj_factor']
    # ref_date = '20170929'
    # print(postgresql_day(fields = field, ref_date = ref_date ))

    # field = ['S_INFO_WINDCODE', 'S_DQ_CLOSE', 'S_DQ_ADJFACTOR']
    # day = '20170929'
    # print(wind_day(field, ref_date=day))


    # field = ['close', 'adj_factor']
    # start_date = '20160129'
    # end_date = '20171121'
    # print(postgresql(fields=field, start_date=start_date, end_date=end_date))

    field = ['S_INFO_WINDCODE', 'S_DQ_CLOSE', 'S_DQ_ADJFACTOR']
    start_date = '20030129'
    end_date = '20031121'
    print(wind(field, start_date = start_date, end_date = end_date))


    # from ct_manager.pg import PGDataHandler
    #
    # handler = PGDataHandler()
    # print (handler.df_read_sql("select * from ashare_eod where trade_date > '20170929' and trade_date < '20171121' "))

    t2 = time.time()
    print(t2 - t1)


