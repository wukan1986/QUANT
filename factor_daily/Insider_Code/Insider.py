# -*- coding: UTF-8 -*-

import cx_Oracle
import pandas as pd
import numpy as np
import os
import math
from scipy.stats import norm
import GetOracle as orl
import GetTradeDay as td
import GetMarketData as md

# Prepare data
def GetInsiderData(BegT, EndT):
    sql = "select s_info_windcode,reported_trader_name,change_volume,trade_avg_price,trade_dt,ann_dt " \
          "from AShareInsiderTrade where (ann_dt>='%s') and (ann_dt<='%s')" % (BegT, EndT)
    data = orl.loaddata('wind', sql)
    data.columns = ['CODE', 'NAME', 'VOLUME', 'PRICE', 'TRADEDATE', 'DATE']
    data['NAME'] = data['NAME'].apply(lambda x: x.decode('gbk'))
    A = md.GetAllStocks_byDate(EndT)
    A = A[A.LISTDATE < BegT].reset_index(drop=True)
    data = data[data['CODE'].isin(A['S_INFO_WINDCODE'])].reset_index(drop=True)
    print('NumberOfStocks = ' + str(len(set(data.CODE))))

    missdata = data[(pd.isnull(data.PRICE)) | (data.PRICE == 0)].reset_index(drop=True)
    missdata = missdata.drop('PRICE', axis=1)
    sql = "select trade_dt,s_info_windcode,s_dq_close from AShareEODPrices where (trade_dt>='%s') and (trade_dt<='%s')"\
          % (missdata['TRADEDATE'].min(), missdata['TRADEDATE'].max())
    close = orl.loaddata('wind', sql)
    close.columns = ['TRADEDATE', 'CODE', 'PRICE']
    missdata = pd.merge(missdata, close, how='left', on=['TRADEDATE', 'CODE'])
    missdata = missdata[pd.isnull(missdata.PRICE) == False]
    data = data[(pd.isnull(data.PRICE) == False) & (data.PRICE != 0)]
    data = pd.concat([data, missdata], axis=0)
    data = data.reset_index(drop=True)
    data['AMOUNT'] = data['PRICE'] * data['VOLUME']

    sql = "select s_info_windcode,trade_dt,tot_shr_today,float_a_shr_today from AShareEODDerivativeIndicator " \
          "where (trade_dt>='%s') and (trade_dt<='%s')" % (data['TRADEDATE'].min(), data['TRADEDATE'].max())
    share = orl.loaddata('wind', sql)
    share.columns = ['CODE', 'TRADEDATE', 'TOTALSHARE', 'FLOATSHARE']
    data = pd.merge(data, share, how='left', on=['CODE', 'TRADEDATE'])
    data = data[(pd.isnull(data.FLOATSHARE) == False) & (data.FLOATSHARE != 0)].reset_index(drop=True)
    data['PCT'] = data['VOLUME'] / data['FLOATSHARE'] / 10000.0
    return data

def GetShareMjrholderData(BegT, EndT):
    sql = "select s_info_windcode,holder_name,transact_type,transact_quantity,avg_price,ann_dt," \
          "transact_startdate,transact_enddate from AShareMjrHolderTrade " \
          "where (ann_dt>'%s') and (ann_dt<'%s') order by ann_dt" % (BegT, EndT)
    data = orl.loaddata('wind', sql)
    data.columns = ['CODE', 'NAME', 'TRANSTYPE', 'VOLUME', 'PRICE', 'DATE', 'TRADEBEGT', 'TRADEENDT']
    data['TRANSTYPE'] = data['TRANSTYPE'].apply(lambda x: x.decode('gbk'))
    data['NAME'] = data['NAME'].apply(lambda x: x.decode('gbk'))
    A = md.GetAllStocks_byDate(EndT)
    A = A[A.LISTDATE < BegT].reset_index(drop=True)
    data = data[data['CODE'].isin(A['S_INFO_WINDCODE'])].reset_index(drop=True)
    print('NumberOfStocks = ' + str(len(set(data.CODE))))

    data.loc[data.TRANSTYPE == u'减持', 'VOLUME'] = (-1) * data.loc[data.TRANSTYPE == u'减持', 'VOLUME']
    data.loc[pd.isnull(data.TRADEBEGT), 'TRADEBEGT'] = data.loc[pd.isnull(data.TRADEBEGT), 'TRADEENDT']
    data['days'] = pd.to_datetime(data['TRADEENDT'], format='%Y-%m-%d') - pd.to_datetime(data['TRADEBEGT'], format='%Y-%m-%d')
    data['days'] = data['days'].apply(lambda x: x.days)

    missdata = data[(pd.isnull(data.PRICE)) | (data.PRICE == 0)].reset_index(drop=True)
    missdata = missdata.drop('PRICE', axis=1)
    sql = "select trade_dt,s_info_windcode,s_dq_close from AShareEODPrices where (trade_dt>='%s') and (trade_dt<='%s')" % (
        missdata['TRADEENDT'].min(), missdata['TRADEENDT'].max())
    close = orl.loaddata('wind', sql)
    close.columns = ['TRADEENDT', 'CODE', 'PRICE']
    missdata = pd.merge(missdata, close, how='left', on=['TRADEENDT', 'CODE'])
    # price = [md.StockAvgClosePrice(missdata.ix[i, 'CODE'], missdata.ix[i, 'TRADEBEGT'], missdata.ix[i, 'TRADEENDT']) for i in range(len(missdata))]
    # missdata['PRICE'] = pd.Series(price, index=missdata.index)
    missdata = missdata[pd.isnull(missdata.PRICE) == False]
    data = data[(pd.isnull(data.PRICE) == False) & (data.PRICE != 0)]
    data = pd.concat([data, missdata], axis=0)
    data = data.reset_index(drop=True)
    data['AMOUNT'] = data['VOLUME'] * data['PRICE']
    return data

def GetSharefloatholderData(tdate):
    sql = "select s_info_windcode,ann_dt,s_holder_enddate,s_holder_holdercategory,s_holder_name,s_holder_quantity " \
          "from AShareFloatHolder where (ann_dt>='%s') and (ann_dt<='%s') order by s_info_windcode,ann_dt" % (
           td.tdaysoffset(tdate, -2, 'Y'), tdate)
    floatdata = orl.loaddata('wind', sql)
    floatdata.columns = ['CODE', 'ANNDATE', 'REPDATE', 'TYPE', 'NAME', 'VOLUME']
    floatdata['NAME'] = floatdata['NAME'].apply(lambda x: x.decode('gbk'))
    floatdata['MD'] = floatdata['REPDATE'].apply(lambda x: x[4:8])
    floatdata = floatdata[floatdata['MD'].isin(['0331', '0630', '0930', '1231'])].reset_index(drop=True)
    A = md.GetAllStocks_byDate(tdate)
    floatdata = floatdata[(floatdata['CODE'].isin(A['S_INFO_WINDCODE']))].reset_index(drop=True)
    repdate = floatdata['REPDATE'].groupby(floatdata['CODE']).max()
    print('NumberOfStocks = ' + str(len(repdate)))
    rep = pd.DataFrame({'CODE': list(repdate.index), 'REPDATE': list(repdate)})
    rep['REPLAST'] = rep['REPDATE'].apply(lambda x: td.lastreportdate(x))

    # Method-1
    data = pd.DataFrame()
    for i in range(len(rep)):
        datai = floatdata[(floatdata.CODE == rep.ix[i, 'CODE']) & (floatdata.REPDATE == rep.ix[i, 'REPDATE'])]
        datalasti = floatdata[(floatdata.CODE == rep.ix[i, 'CODE']) & (floatdata.REPDATE == rep.ix[i, 'REPLAST'])]
        data = pd.concat([data, datalasti, datai], axis=0)
    data = data.reset_index(drop=True)
    sql = "select s_info_windcode,trade_dt,tot_shr_today,float_a_shr_today from AShareEODDerivativeIndicator " \
          "where trade_dt in " + str(tuple(set(data.REPDATE)))
    share = orl.loaddata('wind', sql)
    share.columns = ['CODE', 'REPDATE', 'TOTALSHARE', 'FLOATSHARE']
    data = pd.merge(data, share, how='left', on=['CODE', 'REPDATE'])
    data = data[(pd.isnull(data.FLOATSHARE) == False) & (data.FLOATSHARE != 0)].reset_index(drop=True)
    data.loc[data.TYPE == '2', 'PCT'] = data.loc[data.TYPE == '2', 'VOLUME'] / data.loc[data.TYPE == '2', 'FLOATSHARE'] / 10000.0
    data = data.fillna(0)

    # Method-2
    # sql = "select s_info_windcode,report_period,ann_date,s_holder_holdercategory,s_holder_name,s_holder_quantity," \
    #       "s_holder_pct from AShareinstHolderDerData where (report_period>='%s') and (report_period<='%s')" % (
    #        td.tdaysoffset(repdate.min(), -2, 'Y'), repdate.max())
    # insdata = orl.loaddata('wind', sql)
    # insdata.columns = ['CODE', 'REPDATE', 'ANNDATE', 'COMTYPE', 'NAME', 'VOLUME', 'PCT']
    # insdata['PCT'] = insdata['PCT'] / 100
    # data = pd.DataFrame()
    # for i in range(len(rep)):
    #     datai = floatdata[(floatdata.CODE == rep.ix[i, 'CODE']) & (floatdata.REPDATE == rep.ix[i, 'REPDATE'])]
    #     insi = insdata[(insdata.CODE == rep.ix[i, 'CODE']) & (insdata.REPDATE == rep.ix[i, 'REPDATE'])]
    #     datai = pd.merge(datai, insi[['COMTYPE', 'PCT', 'VOLUME']], how='left', on=['VOLUME'])
    #     datai = datai.drop_duplicates(['NAME'])
    #     datalasti = floatdata[(floatdata.CODE == rep.ix[i, 'CODE']) & (floatdata.REPDATE == rep.ix[i, 'REPLAST'])]
    #     if len(datalasti) > 0:
    #         inslasti = insdata[(insdata.CODE == rep.ix[i, 'CODE']) & (insdata.REPDATE == rep.ix[i, 'REPLAST'])]
    #         datalasti = pd.merge(datalasti, inslasti[['COMTYPE', 'PCT', 'VOLUME']], how='left', on=['VOLUME'])
    #         datalasti = datalasti.drop_duplicates(['NAME'])
    #     data = pd.concat([data, datalasti, datai], axis=0)
    # data = data.reset_index(drop=True)
    # data = data.fillna(0)
    return data

def GetAETTM(data):
    res = data.reset_index(drop=True)
    res = res.sort_values(by=['REPDATE'], ascending=True, inplace=False).reset_index(drop=True)
    for k in range(len(res)-1):
        res.at[k+1, 'AE'] = (res.ix[k+1, 'NOA'] - res.ix[k, 'NOA']) / float(res.ix[k+1, 'ASSET'] + res.ix[k, 'ASSET']) * 2
    res = res.loc[1:len(res), :]
    res = res.reset_index(drop=True)
    ae = pd.DataFrame()
    ae.at[0, 'CODE'] = res.ix[0, 'CODE']
    ae.at[0, 'AE_TTM'] = res['AE'].sum()
    return ae

def GetAccrualEarningData(tdate):
    sql = "select s_info_windcode,report_period,Actual_ann_dt,tot_assets,tot_liab," \
          "monetary_cap,tradable_fin_assets,non_cur_assets_due_within_1y,long_term_eqy_invest,held_to_mty_invest,fin_assets_avail_for_sale,invest_real_estate," \
          "st_borrow,tradable_fin_liab,non_cur_liab_due_within_1y,lt_borrow,bonds_payable from AShareBalanceSheet where " \
          "(statement_type='%s') and ((Actual_ann_dt<='%s') or (ann_dt <='%s'))" % (
              '408001000', tdate, tdate)
    data = orl.loaddata('wind', sql)
    data.columns = ['CODE', 'REPDATE', 'ANNDATE', 'ASSET', 'LIAB',
                    'MONEY', 'TA', '1YA', 'LA', 'TMA', 'SA', 'EA',
                    'SL', 'TL', '1YL', 'LL', 'BL']
    data = data.fillna(0)
    A = md.GetAllStocks_byDate(tdate)
    data = data[data['CODE'].isin(A['S_INFO_WINDCODE'])]
    data = data.reset_index(drop=True)
    data['ASSET_ADJ'] = data['ASSET'] - data['MONEY'] - data['TA'] - data['1YA'] - data['LA'] - data['TMA'] - data['SA']-data['EA']
    data['LIAB_ADJ'] = data['LIAB'] - data['SL'] - data['1YL'] - data['LL'] - data['BL']
    data['NOA'] = data['ASSET_ADJ'] - data['LIAB_ADJ']
    repdate = data['REPDATE'].groupby(data['CODE']).max()
    print('NumberOfStocks = ' + str(len(repdate)))

    rep = pd.DataFrame({'CODE': list(repdate.index), 'REPDATE': list(repdate)})
    rep['REPLY'] = rep['REPDATE'].apply(lambda x: str(int(x[0:4]) - 1) + x[4:8])
    aedata = pd.DataFrame()
    for i in range(len(rep)):
        datai = data[(data.CODE == rep.ix[i, 'CODE']) & (data.REPDATE <= rep.ix[i, 'REPDATE']) & (data.REPDATE >= rep.ix[i, 'REPLY'])]
        if len(datai) == 5:
            datai = GetAETTM(datai)
            aedata = pd.concat([aedata, datai], axis=0)
    aedata = aedata.reset_index(drop=True)
    sw = md.GetAllStockSWInd_byDate(tdate)
    sw.columns = ['CODE', 'INDCODE', 'ENTRY', 'REMOVE']
    aedata = pd.merge(aedata, sw[['CODE', 'INDCODE']], how='left', on=['CODE'])
    return aedata

# Calculate factors
def Splist(data, n):
    return [data[i:i + n] for i in range(len(data)) if i % n == 0]

def Weight(Nmonths, Halflife):
    a = Nmonths*30*np.log(0.5)/Halflife
    b = np.exp(a)
    return b

def Weight2(tdate, data, halflife):
    data_buy = data[data.VOLUME > 0]
    res = pd.Series()
    if (len(data_buy) > 0):
        data['nmonths'] = data['DATE'].apply(lambda x: td.tmonthscount(x, tdate))
        data['weight_it'] = data['nmonths'].apply(lambda x: Weight(x, halflife))
        data['wgtamt'] = data['weight_it'] * data['AMOUNT']
        wgtamt = data['wgtamt'].groupby([data['NAME'], data['nmonths']]).sum()
        amt = data['AMOUNT'].groupby([data['NAME'], data['nmonths']]).sum()
        if np.abs(amt).sum() != 0: res['weight'] = np.abs(wgtamt).sum() / float(np.abs(amt).sum())
        else: res['weight'] = 0
        res['net'] = data['AMOUNT'].sum()
    else:
        res['weight'] = None
        res['net'] = None
    return res

def Weight3(tdate, data, halflife):
    res = pd.Series()
    if (len(data) > 0):
        data['nmonths'] = data['DATE'].apply(lambda x: td.tmonthscount(x, tdate))
        data['weight_it'] = data['nmonths'].apply(lambda x: Weight(x, halflife))
        data['wgtamt'] = data['weight_it'] * data['AMOUNT']
        wgtamt = data['wgtamt'].groupby([data['NAME'], data['nmonths']]).sum()
        amt = data['AMOUNT'].groupby([data['NAME'], data['nmonths']]).sum()
        for k in range(len(data)):
            if data.ix[k, 'days'] >= 182: data.at[k, 'TRANSTYPE'] = u'增减持'
        data_buy = data[(data.TRANSTYPE == u'增持') | (data.TRANSTYPE == u'增减持')]
        data_sell = data[(data.TRANSTYPE == u'减持') | (data.TRANSTYPE == u'增减持')]
        if np.abs(amt).sum() != 0: res['weight'] = np.abs(wgtamt).sum() / float(np.abs(amt).sum())
        else: res['weight'] = 0
        res['times'] = (len(data_buy) - len(data_sell)) / (float(len(data_buy)) + float(len(data_sell)))
    else:
        res['weight'] = None
        res['times'] = None
    return res

def Weight4(tdate, data, halflife):
    res = pd.Series()
    if (len(data) > 0):
        data['nmonths'] = data['DATE'].apply(lambda x: td.tmonthscount(x, tdate))
        data['weight_it'] = data['nmonths'].apply(lambda x: Weight(x, halflife))
        data['wgtamt'] = data['weight_it'] * data['AMOUNT']
        wgtamt = data['wgtamt'].groupby([data['NAME'], data['nmonths']]).sum()
        amt = data['AMOUNT'].groupby([data['NAME'], data['nmonths']]).sum()
        if np.abs(amt).sum() != 0: res['weight'] = np.abs(wgtamt).sum() / float(np.abs(amt).sum())
        else: res['weight'] = 0
        res['net'] = data['AMOUNT'].sum()
    else:
        res['weight'] = None
        res['net'] = None
    return res

def Weight5(tdate, data, halflife):
    data_buy = data[data.VOLUME > 0]
    res = pd.Series()
    if (len(data_buy) > 0):
        data['nmonths'] = data['DATE'].apply(lambda x: td.tmonthscount(x, tdate))
        data['weight_it'] = data['nmonths'].apply(lambda x: Weight(x, halflife))
        data['wgtvol'] = data['weight_it'] * data['VOLUME']
        wgtvol = data['wgtvol'].groupby([data['NAME'], data['nmonths']]).sum()
        vol = data['VOLUME'].groupby([data['NAME'], data['nmonths']]).sum()
        if np.abs(vol).sum() != 0: res['weight'] = np.abs(wgtvol).sum() / float(np.abs(vol).sum())
        else: res['weight'] = 0
        res['net'] = data['VOLUME'].sum()
    else:
        res['weight'] = None
        res['net'] = None
    return res

def AEGroup(data, indtype, topQ, bomQ):
    # Method-1
    # AE = data.reset_index(drop=True)
    # ind = list(set(AE[indtype]))
    # aedata = pd.DataFrame()
    # for i in range(len(ind)):
    #     AEi = AE[AE[indtype] == ind[i]].sort_values(by=['AE'], ascending=False, inplace=False)
    #     top = AEi.head(math.ceil(len(AEi) * topQ)).reset_index(drop=True)
    #     top['GROUP'] = 'TOP'
    #     bottom = AEi.tail(math.ceil(len(AEi) * bomQ)).reset_index(drop=True)
    #     bottom['GROUP'] = 'BOTTOM'
    #     aedata = pd.concat([aedata, top, bottom], axis=0)
    # aedata = aedata.reset_index(drop=True)

    # Method-2
    AE = data.sort_values(by=['AE'], ascending=False, inplace=False).reset_index(drop=True)
    top = AE.head(int(math.ceil(len(AE) * topQ))).reset_index(drop=True)
    top['GROUP'] = 'TOP'
    bottom = AE.tail(int(math.ceil(len(AE) * bomQ))).reset_index(drop=True)
    bottom['GROUP'] = 'BOTTOM'
    aedata = pd.concat([top, bottom], axis=0).reset_index(drop=True)
    return aedata

def NSTGroup(data, sizetype, sizeQ, bomQ):
    # Method-1
    # NST = data.reset_index(drop=True)
    # top = NST[NST.NST > 0].reset_index(drop=True)
    # top['GROUP'] = 'TOP'
    # bottom = pd.DataFrame()
    # NST = NST[NST.NST < 0].sort_values(by=[sizetype], ascending=True, inplace=False).reset_index(drop=True)
    # NST = Splist(NST, math.ceil(len(NST) / sizeQ))
    # for i in range(sizeQ - 1):
    #     NSTi = NST[i].sort_values(by=['NST'], ascending=True, inplace=False)
    #     NSTi = NSTi.head(math.ceil(len(NSTi) * bomQ))
    #     bottom = pd.concat([bottom, NSTi], axis=0)
    # bottom = bottom.reset_index(drop=True)
    # bottom['GROUP'] = 'BOTTOM'
    # nstdata = pd.concat([top, bottom], axis=0)
    # nstdata = nstdata.reset_index(drop=True)

    # Method-2
    NST = data.reset_index(drop=True)
    top = NST[NST.NST > 0].reset_index(drop=True)
    top['GROUP'] = 'TOP'
    bottom = NST[NST.NST < 0].sort_values(by=['NST'], ascending=True, inplace=False).reset_index(drop=True)
    bottom = bottom.head(int(math.ceil(len(NST) * bomQ)))
    bottom['GROUP'] = 'BOTTOM'
    nstdata = pd.concat([top, bottom], axis=0)
    nstdata = nstdata.reset_index(drop=True)
    return nstdata

def GetPV(tdate, N, Fre, halflife, datapath):
    begt = td.tdaysoffset(tdate, N, Fre)
    endt = tdate
    print(begt + '--' + endt)
    data = pd.read_csv(datapath + 'InsiderData_' + tdate + '.csv', encoding='gbk')
    data['DATE'] = data['DATE'].apply(lambda x: str(x))
    # data = GetInsiderData(begt, endt)

    A = list(set(data.CODE))
    PV = pd.DataFrame()
    for i in range(len(A)):
        # print(A[i])
        datai = data[data.CODE == A[i]].reset_index(drop=True)
        w = Weight2(tdate, datai, halflife)
        PV.at[i, 'S_INFO_WINDCODE'] = A[i][0:6] + '-CN'
        PV.at[i, 'weight'] = w['weight']
        PV.at[i, 'net'] = w['net']
        # print(PV.ix[i, 'weight'])
        # print(PV.ix[i, 'net'])
    PV = PV[pd.isnull(PV.weight) == False].reset_index(drop=True)
    PV['normal'] = (PV['net']-PV['net'].mean())/PV['net'].std()
    PV['CDF'] = PV['normal'].apply(lambda x: norm.cdf(x))
    PV['PV_raw'] = PV['CDF'] * PV['weight']
    return PV

def GetNST(tdate, N, Fre, halflife, datapath):
    begt = td.tdaysoffset(tdate, N, Fre)
    endt = tdate
    print(begt + '--' + endt)
    data = pd.read_csv(datapath + 'InsiderData_' + tdate + '.csv', encoding='gbk')
    data['DATE'] = data['DATE'].apply(lambda x: str(x))
    # data = GetInsiderData(begt, endt)

    A = list(set(data.CODE))
    NST = pd.DataFrame()
    for i in range(len(A)):
        # print(A[i])
        datai = data[data.CODE == A[i]].reset_index(drop=True)
        w = Weight4(tdate, datai, halflife)
        NST.at[i, 'S_INFO_WINDCODE'] = A[i][0:6] + '-CN'
        NST.at[i, 'NST'] = datai['PCT'].sum()
        NST.at[i, 'weight'] = w['weight']
        NST.at[i, 'SIZE'] = datai.ix[0, 'SIZE']
        # print(NST.ix[i, 'NST'])
        # print(NST.ix[i, 'weight'])
    NST = NST[pd.isnull(NST.weight) == False].reset_index(drop=True)
    NST['NST_normal'] = (NST['NST'] - NST['NST'].mean()) / NST['NST'].std()
    NST['NST_raw'] = NST['NST_normal'].apply(lambda x: norm.cdf(x))

    # path_neut = 'D:/work/Factors/Insider/NSTS/'
    # NST_neut = pd.read_csv(path_neut + 'NSTS_neut_CN_' + tdate + '.csv', encoding='gbk')
    # NST = pd.merge(NST[['S_INFO_WINDCODE', 'weight', 'NST_raw']], NST_neut, how='inner', on=['S_INFO_WINDCODE'])
    # NST.rename(columns={'NSTS_neut': 'NST'}, inplace=True)
    return NST

def GetAE(tdate, datapath):
    AE = pd.read_csv(datapath + 'AccrualEarningData_' + tdate + '.csv', encoding='gbk')
    # AE = GetAccrualEarningData(tdate)
    AE['S_INFO_WINDCODE'] = AE['CODE'].apply(lambda x: x[0:6] + '-CN')
    AE['AE'] = AE['AE_TTM']
    AE['AE_normal'] = (AE['AE'] - AE['AE'].mean()) / AE['AE'].std()
    AE['AE_raw'] = AE['AE_normal'].apply(lambda x: norm.cdf(x))
    AE['AE_raw'] = AE['AE_raw'] * (-1)

    # path_neut = 'D:/work/Factors/Insider/AES/'
    # AE_neut = pd.read_csv(path_neut + 'AES_neut_CN_' + tdate + '.csv', encoding='gbk')
    # AE = pd.merge(AE[['S_INFO_WINDCODE', 'AE_raw']], AE_neut, how='inner', on=['S_INFO_WINDCODE'])
    # AE.rename(columns={'AES_neut': 'AE'}, inplace=True)
    return AE

def GetSA(tdate, N, Fre, halflife, para, datapath):
    begt = td.tdaysoffset(tdate, N, Fre)
    endt = tdate
    print(begt + '--' + endt)
    AE = GetAE(tdate, datapath)
    NST = GetNST(tdate, N, Fre, halflife, datapath)
    cap = md.GetAllStocks_byDate(tdate)
    cap['S_INFO_WINDCODE'] = cap['S_INFO_WINDCODE'].apply(lambda x: x[0:6] + '-CN')
    NST = pd.merge(NST, cap[['S_INFO_WINDCODE', 'FLOATMV']], how='left', on=['S_INFO_WINDCODE'])

    AEG = AEGroup(AE, para['indtype'], para['aetopQ'], para['aebomQ'])  # BARAIND / INDCODE
    AE_top = AEG[AEG.GROUP == 'TOP'].reset_index(drop=True)
    AE_bottom = AEG[AEG.GROUP == 'BOTTOM'].reset_index(drop=True)
    NSTG = NSTGroup(NST, para['sizetype'], para['sizeQ'], para['nstbomQ'])  # SIZE / FLOATMV
    NST_top = NSTG[NSTG.GROUP == 'TOP'].reset_index(drop=True)
    NST_bottom = NSTG[NSTG.GROUP == 'BOTTOM'].reset_index(drop=True)

    SA1 = pd.merge(AE_top[['S_INFO_WINDCODE', 'AE']], NST_bottom[['S_INFO_WINDCODE', 'NST', 'weight']], how='inner', on=['S_INFO_WINDCODE'])
    SA2 = pd.merge(AE_bottom[['S_INFO_WINDCODE', 'AE']], NST_top[['S_INFO_WINDCODE', 'NST', 'weight']], how='inner', on=['S_INFO_WINDCODE'])
    SA = pd.concat([SA1, SA2], axis=0)
    SA = SA.drop_duplicates(['S_INFO_WINDCODE']).reset_index(drop=True)
    SA['AE'] = SA['AE'] * (-1)
    SA['AE_normal'] = (SA['AE'] - SA['AE'].mean()) / SA['AE'].std()
    SA['NST_normal'] = (SA['NST'] - SA['NST'].mean()) / SA['NST'].std()
    SA['AE_CDF'] = SA['AE_normal'].apply(lambda x: norm.cdf(x))
    SA['NST_CDF'] = SA['NST_normal'].apply(lambda x: norm.cdf(x))
    SA['SA_raw'] = np.sum(SA[['AE_CDF', 'NST_CDF']], axis=1) * SA['weight']
    SA['Accrualwgt_raw'] = SA['AE_CDF'] * SA['weight']
    SA['Accrual_raw'] = SA['AE_CDF']
    SA['SL_raw'] = SA['NST_CDF']
    return SA

def GetMJRR(tdate, N, Fre, halflife, datapath):
    begt = td.tdaysoffset(tdate, N, Fre)
    endt = tdate
    print(begt + '--' + endt)
    data = pd.read_csv(datapath + 'ShareMjrholderData_' + tdate + '.csv', encoding='gbk')
    data['DATE'] = data['DATE'].apply(lambda x: str(x))
    # data = GetShareMjrholderData(begt, endt)

    A = list(set(data.CODE))
    MJRR = pd.DataFrame()
    for i in range(len(A)):
        # print(A[i])
        datai = data[data.CODE == A[i]].reset_index(drop=True)
        w = Weight3(tdate, datai, halflife)
        MJRR.at[i, 'S_INFO_WINDCODE'] = A[i][0:6] + '-CN'
        MJRR.at[i, 'weight'] = w['weight']
        MJRR.at[i, 'times'] = w['times']
        # print(MJRR.ix[i, 'weight'])
        # print(MJRR.ix[i, 'times'])
    MJRR = MJRR[pd.isnull(MJRR.weight) == False].reset_index(drop=True)
    MJRR['MJRR_raw'] = MJRR['weight'] * MJRR['times']
    return MJRR

def GetMJRV(tdate, N, Fre, halflife, datapath):
    begt = td.tdaysoffset(tdate, N, Fre)
    endt = tdate
    print(begt + '--' + endt)
    data = pd.read_csv(datapath + 'ShareMjrholderData_' + tdate + '.csv', encoding='gbk')
    data['DATE'] = data['DATE'].apply(lambda x: str(x))
    # data = GetShareMjrholderData(begt, endt)

    A = list(set(data.CODE))
    MJRV = pd.DataFrame()
    for i in range(len(A)):
        # print(A[i])
        datai = data[data.CODE == A[i]].reset_index(drop=True)
        w = Weight5(tdate, datai, halflife)
        MJRV.at[i, 'S_INFO_WINDCODE'] = A[i][0:6] + '-CN'
        MJRV.at[i, 'weight'] = w['weight']
        MJRV.at[i, 'net'] = w['net']
        # print(MJRV.ix[i, 'weight'])
        # print(MJRV.ix[i, 'net'])
    MJRV = MJRV[pd.isnull(MJRV.weight) == False].reset_index(drop=True)
    MJRV['normal'] = (MJRV['net']-MJRV['net'].mean())/MJRV['net'].std()
    MJRV['CDF'] = MJRV['normal'].apply(lambda x: norm.cdf(x))
    MJRV['MJRV_raw'] = MJRV['CDF'] * MJRV['weight']
    return MJRV

def GetLIO(tdate, datapath):
    data = pd.read_csv(datapath + 'SharefloatholderData_' + tdate + '.csv', encoding='gbk')
    # data = GetSharefloatholderData(tdate)

    A = list(set(data.CODE))
    LIO = pd.DataFrame()
    for i in range(len(A)):
        # print(A[i])
        datai = data[data.CODE == A[i]]
        datai = datai[datai.REPDATE == datai['REPDATE'].max()].dropna(axis=0, how='any')
        LIO.at[i, 'S_INFO_WINDCODE'] = A[i][0:6] + '-CN'
        LIO.at[i, 'weight'] = datai['PCT'].sum()
        # print(LIO.ix[i, 'weight'])
    LIO['LIO_raw'] = np.log(1 + LIO['weight']) * (-1)
    return LIO

def GetLIOCHANGE(tdate, datapath):
    data = pd.read_csv(datapath + 'SharefloatholderData_' + tdate + '.csv', encoding='gbk')
    data['REPDATE'] = data['REPDATE'].apply(lambda x: str(x))
    # data = GetSharefloatholderData_2P(tdate)

    A = list(set(data.CODE))
    LIOChg = pd.DataFrame()
    for i in range(len(A)):
        # print(A[i])
        datai = data[data.CODE == A[i]].reset_index(drop=True)
        datai = datai['PCT'].groupby(datai['REPDATE']).sum()
        if len(datai) == 2:
            # print(datai[0])
            # print(datai[1])
            LIOChg.at[i, 'S_INFO_WINDCODE'] = A[i][0:6] + '-CN'
            LIOChg.at[i, 'weight_last'] = datai[0]
            LIOChg.at[i, 'weight_t'] = datai[1]
    LIOChg = LIOChg.reset_index(drop=True)
    LIOChg['LIOChg_last'] = np.log(1 + LIOChg['weight_last'])
    LIOChg['LIOChg_t'] = np.log(1 + LIOChg['weight_t'])
    LIOChg.loc[LIOChg.LIOChg_last != 0, 'LIOCHANGE_raw'] = LIOChg.loc[LIOChg.LIOChg_last != 0, 'LIOChg_t'] / LIOChg.loc[LIOChg.LIOChg_last != 0, 'LIOChg_last'] -1
    LIOChg.loc[(LIOChg.LIOChg_last == 0) & (LIOChg.LIOChg_t != 0), 'LIOCHANGE_raw'] = LIOChg.loc[LIOChg.LIOChg_last != 0, 'LIOCHANGE_raw'].max()
    LIOChg.loc[(LIOChg.LIOChg_last == 0) & (LIOChg.LIOChg_t == 0), 'LIOCHANGE_raw'] = None
    LIOChg = LIOChg[pd.isnull(LIOChg.LIOCHANGE_raw) == False].reset_index(drop=True)
    LIOChg['LIOCHANGE_raw'] = LIOChg['LIOCHANGE_raw'] * (-1)
    return LIOChg

# Main
def LoadInsiderData_main(dateslist, N, Fre, datapath, barapath):
    for t in range(len(dateslist)):
        # print(dateslist[t])
        tdate = dateslist[t]
        begt = td.tdaysoffset(tdate, N, Fre)
        endt = tdate
        print(begt + '--' + endt)

        BaraData = pd.read_csv(barapath + 'Exposure_' + tdate + '.csv', skiprows=1)
        if len(BaraData) == 0:
            return 'Bara file does NOT exist !'
        else:
            print('BaraData')
            print('NumberOfStocks = ' + str(len(BaraData)))
            RF = ['BETA', 'MOMENTUM', 'SIZE', 'EARNYILD', 'RESVOL', 'GROWTH', 'BTOP', 'LEVERAGE', 'LIQUIDTY', 'SIZENL',
                  'COUNTRY', 'Unnamed: 44']
            BaraData.rename(columns={'Unnamed: 0': 'S_INFO_WINDCODE'}, inplace=True)
            BaraSize = BaraData[['S_INFO_WINDCODE', 'SIZE']]
            IndData = BaraData.drop(RF, axis=1, inplace=False)
            ind = list(IndData.columns)
            BaraInd = pd.DataFrame()
            for i in range(len(ind) - 1):
                temp = IndData[IndData[ind[i + 1]] == 1].reset_index(drop=True)
                temp = temp[['S_INFO_WINDCODE']]
                temp['BARAIND'] = ind[i + 1]
                BaraInd = pd.concat([BaraInd, temp], axis=0)
            BaraInd = BaraInd.reset_index(drop=True)

            print('InsiderData')
            InsiderData = GetInsiderData(begt, endt)
            InsiderData['S_INFO_WINDCODE']=InsiderData['CODE'].apply(lambda x: x[0:6] + '-CN')
            InsiderData = pd.merge(InsiderData, BaraSize, how='left', on=['S_INFO_WINDCODE'])
            InsiderData.to_csv(datapath + 'InsiderData_' + tdate + '.csv', index=None, encoding='gbk')

            print('ShareMjrholderData')
            ShareMjrholderData = GetShareMjrholderData(begt, endt)
            ShareMjrholderData.to_csv(datapath + 'ShareMjrholderData_' + tdate + '.csv', index=None, encoding='gbk')

            print('SharefloatholderData')
            SharefloatholderData = GetSharefloatholderData(tdate)
            SharefloatholderData.to_csv(datapath + 'SharefloatholderData_' + tdate + '.csv', index=None, encoding='gbk')

            print('AccrualEarningData')
            AccrualEarningData = GetAccrualEarningData(tdate)
            AccrualEarningData['S_INFO_WINDCODE'] = AccrualEarningData['CODE'].apply(lambda x: x[0:6] + '-CN')
            AccrualEarningData = pd.merge(AccrualEarningData, BaraInd, how='left', on=['S_INFO_WINDCODE'])
            AccrualEarningData.to_csv(datapath + 'AccrualEarningData_' + tdate + '.csv', index=None, encoding='gbk')
    return 'Insider_data_finished!'

def GetInsider_main(dateslist, N, Fre, Halflife, SApara, datapath, outputpath):
    for t in range(len(dateslist)):
        print(dateslist[t])

        print('****** PV ******')
        PV = GetPV(dateslist[t], N, Fre, Halflife, datapath)
        PV = PV[['S_INFO_WINDCODE', 'PV_raw']]
        path = outputpath + 'PV'
        if not os.path.exists(path): os.mkdir(path)
        PV.to_csv(path + '/PV_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** AES ******')
        AES = GetAE(dateslist[t], datapath)
        AES = AES[['S_INFO_WINDCODE', 'AE_raw']]
        AES.columns = ['S_INFO_WINDCODE', 'AES_raw']
        path = outputpath + 'AES'
        if not os.path.exists(path): os.mkdir(path)
        AES.to_csv(path + '/AES_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** NSTS ******')
        NSTS = GetNST(dateslist[t], N, Fre, Halflife, datapath)
        NSTS = NSTS[['S_INFO_WINDCODE', 'NST_raw']]
        NSTS.columns = ['S_INFO_WINDCODE', 'NSTS_raw']
        path = outputpath + 'NSTS'
        if not os.path.exists(path): os.mkdir(path)
        NSTS.to_csv(path + '/NSTS_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** SA/SL/Accrual/Accrualwgt ******')
        SA = GetSA(dateslist[t], N, Fre, Halflife, SApara, datapath)
        SL = SA[['S_INFO_WINDCODE', 'SL_raw']]
        path = outputpath + 'SL'
        if not os.path.exists(path): os.mkdir(path)
        SL.to_csv(path + '/SL_raw_CN_' + dateslist[t] + '.csv', index=None)

        Accrual = SA[['S_INFO_WINDCODE', 'Accrual_raw']]
        path = outputpath + 'Accrual'
        if not os.path.exists(path): os.mkdir(path)
        Accrual.to_csv(path + '/Accrual_raw_CN_' + dateslist[t] + '.csv', index=None)

        Accrualwgt = SA[['S_INFO_WINDCODE', 'Accrualwgt_raw']]
        path = outputpath + 'Accrualwgt'
        if not os.path.exists(path): os.mkdir(path)
        Accrualwgt.to_csv(path + '/Accrualwgt_raw_CN_' + dateslist[t] + '.csv', index=None)

        SA = SA[['S_INFO_WINDCODE', 'SA_raw']]
        path = outputpath + 'SA'
        if not os.path.exists(path): os.mkdir(path)
        SA.to_csv(path + '/SA_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** MJRR ******')
        MJRR = GetMJRR(dateslist[t], N, Fre, Halflife, datapath)
        MJRR = MJRR[['S_INFO_WINDCODE', 'MJRR_raw']]
        path = outputpath + 'MJRR'
        if not os.path.exists(path): os.mkdir(path)
        MJRR.to_csv(path + '/MJRR_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** MJRV ******')
        MJRV = GetMJRV(dateslist[t], N, Fre, Halflife, datapath)
        MJRV = MJRV[['S_INFO_WINDCODE', 'MJRV_raw']]
        path = outputpath + 'MJRV'
        if not os.path.exists(path): os.mkdir(path)
        MJRV.to_csv(path + '/MJRV_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** LIO ******')
        LIO = GetLIO(dateslist[t], datapath)
        LIO = LIO[['S_INFO_WINDCODE', 'LIO_raw']]
        path = outputpath + 'LIO'
        if not os.path.exists(path): os.mkdir(path)
        LIO.to_csv(path + '/LIO_raw_CN_' + dateslist[t] + '.csv', index=None)

        print('****** LIOCHANGE ******')
        LIOCHANGE = GetLIOCHANGE(dateslist[t], datapath)
        LIOCHANGE = LIOCHANGE[['S_INFO_WINDCODE', 'LIOCHANGE_raw']]
        path = outputpath + 'LIOCHANGE'
        if not os.path.exists(path): os.mkdir(path)
        LIOCHANGE.to_csv(path + '/LIOCHANGE_raw_CN_' + dateslist[t] + '.csv', index=None)
    return 'Insider_raw_finished!'


if __name__ =='__main__':
    begt = '20080101'
    endt = '20130630'
    N = -1
    Fre = 'Y'
    halflife = 180
    SApara = {'aetopQ': 0.25, 'aebomQ': 0.5, 'sizeQ': 5, 'nstbomQ': 0.5, 'indtype': 'BARAIND', 'sizetype': 'SIZE'}
    factorname = 'LIO'
    outputpath = 'D:/work/Factors/Insider/'
    datapath = 'D:/work/Factors/Insider/Insider_Data/'
    barapath = 'Z:/axioma_data/barra_model/Exposure/'
    dateslist = td.tdays(begt, endt, 'M')
    # dateslist = ['20171225']
    StockID = '000031.SZ'
    tdate = '20171225'
    begt1 = td.tdaysoffset(tdate, N, Fre)
    endt1 = tdate

    # res = GetInsiderData(begt1, endt1)
    # res = GetShareMjrholderData(begt1, endt1)
    # res = GetAccrualEarningData(tdate)
    # res = GetSharefloatholderData(tdate)
    # res = LoadInsiderData(tdate, N, Fre, datapath, barapath)
    # res = LoadInsiderData_main(tdate, N, Fre, datapath)

    # res = GetMJRR(tdate, N, Fre, halflife, datapath)
    # res = GetNST(tdate, N, Fre, halflife, datapath)
    # res = GetAE(tdate, datapath)
    res = GetSA(tdate, N, Fre, halflife, SApara, datapath)
    # res = GetMJRV(tdate, N, Fre, halflife, datapath)
    # res = GetMJRR(tdate, N, Fre, halflife, datapath)
    # res = GetLIO(tdate, datapath)
    # res = GetLIOCHANGE(tdate, datapath)
    # GetInsider_main(dateslist, N, Fre, halflife, SApara, datapath, outputpath)
    path = 'D:/work/Factors/Insider/sa.csv'
    res.to_csv(path, index=None, encoding='gbk')
    print(res)




