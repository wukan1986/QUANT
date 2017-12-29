# -*- coding: UTF-8 -*-

import cx_Oracle
import uqer
from uqer import DataAPI
import pandas as pd
import sys
import numpy as np

def getbd(type = 'wind'):
    dbSetting = pd.Series()
    if type == 'wind':
        dbSetting['DB_HOST'] = '10.180.10.139:1521/WINDB'
        dbSetting['DB_USER'] = 'rwind'
        dbSetting['DB_PASSWORD'] = 'rwind'
    elif type == 'ctquant2':
        dbSetting['DB_HOST'] = '10.180.10.179:1521/GOGODB'
        dbSetting['DB_USER'] = 'ctquant2'
        dbSetting['DB_PASSWORD'] = 'ctquant2'
    elif type == 'gogo':
        dbSetting['DB_HOST'] = '10.180.10.179:1521/GOGODB'
        dbSetting['DB_USER'] = 'gogousr'
        dbSetting['DB_PASSWORD'] = 'gogousr123'
    return dbSetting

def loaddata(type, sql):
    dbSetting = getbd(type)
    db = cx_Oracle.connect(dbSetting['DB_USER'],dbSetting['DB_PASSWORD'],dbSetting['DB_HOST'])
    data = pd.read_sql(sql, db)
    db.close()
    return data