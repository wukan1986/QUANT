# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 10:40:39 2017

@author: shiyunchao
"""

import pandas as pd
import cx_Oracle
import os

#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class read_db():
    def __init__(self, flag = 'ctquant2'):
        if flag == 'wind':
            self.DB_HOST = '10.180.10.139:1521/WINDB'
            self.DB_USER = 'rwind'
            self.DB_PASSWORD = 'rwind'
        elif flag == 'ctquant2':
            self.DB_HOST = '10.180.10.179:1521/GOGODB'
            self.DB_USER = 'ctquant2'
            self.DB_PASSWORD = 'ctquant2'
    
    def db_query(self,query):
        db=cx_Oracle.connect(self.DB_USER,self.DB_PASSWORD,self.DB_HOST)
        data = pd.read_sql(query, db)
        db.close()
        return data
        

if __name__ == "__main__": 
    start = '20170801'
    end = '20171001'
    sql = "select TRADE_DT,S_INFO_WINDCODE,S_DQ_TRADESTATUS from ASHAREEODPRICES where TRADE_DT>=%s and TRADE_DT<=%s"%(start,end)

    test = read_db()
    df = test.db_query(sql)
    