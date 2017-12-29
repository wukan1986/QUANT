# -*- coding: UTF-8 -*-

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

def main(type):
    begt = '20080101'
    endt = '20171130'
    N = -1
    Fre = 'Y'
    Halflife = 180.0
    SApara = {'aetopQ': 0.25, 'aebomQ': 0.5, 'sizeQ': 5.0, 'nstbomQ': 0.5, 'indtype': 'BARAIND', 'sizetype': 'SIZE'}

    if type == 0:
        # outputpath = 'D:/work/Factors/Insider/'
        outputpath = 'Z:/axioma_data/alpha/'
        dateslist = td.tdays(begt, endt, 'M')
    else:
        # outputpath = 'D:/work/Factors/Insider/'
        outputpath = 'Z:/daily_data/alpha/raw/'
        dateslist = [(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")]

    print(dateslist)
    datapath = 'F:/factor_data/Insider_Data/'
    barapath = 'Z:/axioma_data/barra_model/Exposure/'
    Insider.LoadInsiderData_main(dateslist, N, Fre, datapath, barapath)
    # Insider.GetInsider_main(dateslist, N, Fre, Halflife, SApara, datapath, outputpath)
    return 'Finished!'


schedule.every().day.at("07:00").do(main(1))

while True:
    schedule.run_pending()
    time.sleep(1)
