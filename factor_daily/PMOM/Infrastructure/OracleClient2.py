import cx_Oracle
import numpy as np
import pandas as pd
import os
import Infrastructure.Config as config
import sys
import datetime as dt
import logging


# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class OracleClient:


    def __init__(self, conn_val):
        self.conn = self.__get_conn(conn_val)


    def get(self, sql, columns=None, params = None):
        cursor = self.conn.cursor()
        if params is not None:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        val = cursor.fetchall()
        df = None
        if (val is not None)&(len(val)>0):
            if columns is None:
                tb_name = self.get_tb_name(sql)
                columns = [items[0] for items in self.get_col_info(tb_name)]
            df = pd.DataFrame(val,columns=columns)
        cursor.close()
        return df

    def get_raw(self,sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        val = cursor.fetchall()
        cursor.close()
        return val

    def get_lob_dic(self, columns_info):
        lob_dic = {}
        for info in columns_info:
            lob = info[1].lower()
            if lob in ['clob','nclob','blob']:
                lob_dic[info[0]] = lob
        return lob_dic

    def gene_insert_sql(self, tb_name, columns_info):
        column_str = ''
        for info in columns_info:
            column_str +=info[0]+','
        column_str = column_str.rstrip(',')
        parameters = ''
        for col in [info[0] for info in columns_info]:
            parameters = parameters + ':' + col + ','
        parameters = parameters.rstrip(',')
        return 'insert into {0}({1}) values({2})'.format(tb_name, column_str, parameters)

    def get_tb_name(self,sql):
        start = sql.find('from')
        end = sql.find('where')
        tb = sql[start + 4:].strip() if end ==-1 else sql[start+4:end].strip()
        return tb.split(' ')[0]

    def get_col_info(self, tb_name):
        sql = "select column_name,data_type from user_tab_cols where table_name='{0}' and hidden_column ='NO' order by  column_id asc".format(str.upper(tb_name))
        cursor = self.conn.cursor()
        cursor.execute(sql)
        vals = cursor.fetchall()
        cursor.close()
        return [(item[0], item[1]) for item in vals]

    def get_one(self, sql, para = None):
        cursor = self.conn.cursor()
        if para is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql,para)
        val = cursor.fetchone()
        cursor.close()
        return val[0]

    def insert_bulk(self, sql, df, lob_dic={}):
        cursor = self.conn.cursor()
        if (df is None) or (df.empty==True):
            return
        list_dic_vals = []
        for index, row in df.iterrows():
            row = row.replace({np.nan: None})
            list_dic_vals.append(row.to_dict())
        lobdict = {}
        for k in list_dic_vals[0].keys():
            if k in lob_dic.keys():
                if lob_dic[k].lower() == 'clob':
                    lobdict[k] = cursor.var(cx_Oracle.CLOB, arraysize=len(list_dic_vals))
                elif lob_dic[k].lower() == 'nclob':
                    lobdict[k] = cursor.var(cx_Oracle.NCLOB, arraysize=len(list_dic_vals))
                elif lob_dic[k].lower() == 'blob':
                    lobdict[k] = cursor.var(cx_Oracle.BLOB, arraysize=len(list_dic_vals))

        for rownum, row in enumerate(list_dic_vals):
            for key, val in row.items():
                if key in lobdict.keys():
                    lob = lobdict[key]
                    if val is not None:
                        lob.setvalue(rownum,val.read())
                    else:
                        lob.setvalue(rownum,'')
                    row[key] = lob
        cursor.prepare(sql)
        cursor.executemany(None, list_dic_vals)
        cursor.close()
        self.conn.commit()

    def __get_conn(self, conn_val):
        usr = conn_val['usr']
        psd = conn_val['psd']
        server = '{0}:{1}/{2}'.format(conn_val['ip'], conn_val['port'], conn_val['db'])
        return cx_Oracle.connect(usr, psd, server)

    def __del__(self):
        try:
            self.conn.close()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    conn_val = config.get_db_conn('gogodb')
    print(conn_val)
    source_cli = OracleClient(conn_val)
    #select_sql = "select * from ashareeodprices where s_info_windcode='600000.SH' and trade_dt >='20170101'"
    select_sql = 'select * from P_GG_KEYDATA where tdate>20050101 and tdate<20050103'
    fm = source_cli.get_col_info('CMB_REPORT_RESEARCH')
    print(fm)
    exit(0)

    des_cli = OracleClient(config.get_db_conn('quant2'))
    col_names = des_cli.get_col_info('ASHAREEODPRICES')
    print(col_names)
    insert_sql = 'insert into ashareeodprices values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23)'
    #m[21] = fm[21].dt.strftime('%Y-%M-%d %H:%S:%M')
    #des_cli.insert_bulk(insert_sql,fm.values.tolist())
