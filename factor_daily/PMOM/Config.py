# -*- coding: utf-8 -*-
import configparser
import os

def getConfig(path, section):
    __config = configparser.ConfigParser()
    __config.read(path)
    __items = __config.items(section)
    return {val[0]:val[1] for val in __items}


def get_db_conn(section):
    __current_path = os.path.split(os.path.realpath(__file__))[0]
    __parent = os.path.split(__current_path)[0]
    # __conf_path = __parent + '\\app.conf'
    __conf_path = __current_path + '\\app.conf'
    return getConfig(__conf_path, section)

if __name__ == '__main__':
    __current_path = os.path.split(os.path.realpath(__file__))[0]
    __parent = os.path.split(__current_path)[0]
    print(__current_path, __parent)
    __conf_path = __parent + '\\app.conf'
    print(__conf_path)
    __vals = getConfig(__conf_path, 'gogodb')
    print(__vals['psd'])
