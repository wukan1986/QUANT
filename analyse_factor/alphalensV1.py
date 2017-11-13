# -*- coding: utf-8 -*-
"""
Created on Wed Mar 01 11:04:22 2017

@author: leiton
"""



import alphalens
from alphalens import utils
import pandas as pd
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
from pylab import mpl   #画图显示中文
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False

import uqer
from uqer import DataAPI
client = uqer.Client(token='811e6680b27759e045ed16e2ed9b408dc8a0cbffcf14e4bb755144dd45fa5ea0')

import sys
sys.path.append("..")
from tools import get_tradeDay
reload(sys)



start = '20080401'
end = '20170510'

df_index = DataAPI.MktIdxdGet(tradeDate=u"",indexID=u"",ticker=u"000300",beginDate=start,endDate=end,exchangeCD=u"XSHE,XSHG",field=['tradeDate','closeIndex'],pandas="1")
df_index.columns = ['trade_day','close']

fre = 'month'
tradeday = get_tradeDay.wind(start, end, fre=fre)

factor_name = 'RAND1'
factor_path = 'Z:/axioma_data/alpha/%s'%factor_name
# factor_path = 'E:/QUANT/alpha_factor/risk_data/SIZE_uqer'

close_path = 'Z:/backtest_data/close_data/gogo_data'
df_close = pd.DataFrame([])
for i in tradeday:
    print(i)
    close = pd.read_csv('%s/close_%s.csv'%(close_path, i), dtype={'ticker': str})
    close['trade_day'] = i
    df_close = pd.concat([df_close,close])

df_close = df_close.pivot(index='trade_day', columns='ticker', values='close_s')
df_close.index = pd.to_datetime(df_close.index, format='%Y%m%d')

df_factor = pd.DataFrame([])
for i in tradeday:
    factor = pd.read_csv('%s/%s_raw_CN_%s.csv'%(factor_path,factor_name,i))
    factor['trade_day'] = i
    df_factor = pd.concat([df_factor, factor])
df_factor.columns = ['code',factor_name,'trade_day']

df_factor = df_factor.pivot(index='trade_day', columns='code', values=factor_name)
df_factor.index = pd.to_datetime(df_factor.index, format='%Y%m%d')
df_factor.columns = list(map(lambda x: x.split('-')[0],df_factor.columns.tolist()))

df_dt = pd.DataFrame(tradeday)
df_dt['trade_day']= pd.to_datetime(df_dt['trade_day'], format='%Y%m%d')
df_index['trade_day']= pd.to_datetime(df_index['trade_day'], format='%Y-%m-%d')
df_index = pd.merge(df_dt, df_index, on='trade_day')
df_index['ret'] = df_index['close'].pct_change()
df_index = df_index[['trade_day','ret']]
df_index['ret'] = df_index['ret'].shift(-1).values
df_index = df_index.set_index('trade_day')

col_index = df_index.columns.tolist()[0]
df_benchmark = pd.DataFrame([])
num_group =range(10)
for i in num_group:
    df_benchmark.loc[:,i+1] = df_index[col_index]
df_benchmark = df_benchmark.T.stack()

df_benchmark = pd.DataFrame(df_benchmark)
df_benchmark.columns = [col_index]
df_benchmark.index.names = [u'factor_quantile', u'date']
test = df_benchmark.loc[(df_benchmark.index.get_level_values(u'factor_quantile') ==1)]
test5 = df_benchmark.loc[(df_benchmark.index.get_level_values(u'factor_quantile') ==5)]

factor_init = df_factor.stack()

### 图片保存的地址
filename = 'fig_%s'%factor_name

if not os.path.exists(filename):
    os.mkdir(filename)

# test1 = df_close.isnull()
# df_factor = df_factor[(~test1)]
# df_close = df_close.fillna(method='pad')

#alphalens.tears.create_factor_tear_sheet(factor_init, close)
factor_data = alphalens.utils.get_clean_factor_and_forward_returns(factor_init, df_close,periods=(1,),quantiles=10)


mean_monthly_ic = alphalens.performance.mean_information_coefficient(factor_data, by_time='M')
alphalens.plotting.plot_monthly_ic_heatmap(mean_monthly_ic)
plt.savefig('%s/IC_heatmap.png'%filename)
plt.close()
#


### 事件驱动型
alphalens.tears.create_event_returns_tear_sheet(factor_data, df_close, avgretplot=(20, 20),long_short=False)
plt.savefig('%s/event_drive_returns2.png'%filename)
plt.close()
#### 画累计收益率
mean_return_by_q_daily, std_err = alphalens.performance.mean_return_by_quantile(factor_data, by_date=True,demeaned=False)
col_mean_return = mean_return_by_q_daily.columns.tolist()
active_mean_return_by_q_daily = pd.concat([mean_return_by_q_daily, df_benchmark], join='inner',axis=1)
for ii in col_mean_return:
    active_mean_return_by_q_daily[ii] = active_mean_return_by_q_daily[ii] - active_mean_return_by_q_daily[col_index]
active_mean_return_by_q_daily = active_mean_return_by_q_daily[col_mean_return]
active_mean_return_by_q_daily.index.names = [u'factor_quantile', u'date']


mean_return_by_q, std_err_by_q = alphalens.performance.mean_return_by_quantile(factor_data, by_group=False,demeaned=False)
alphalens.plotting.plot_quantile_returns_bar(mean_return_by_q)
plt.savefig('%s/bar.png'%filename)
plt.close()

active_mean_return_by_q, active_std_err_by_q = alphalens.performance.mean_return_by_quantile(factor_data, by_group=False)
alphalens.plotting.plot_quantile_returns_bar(active_mean_return_by_q)
plt.savefig('%s/active_bar.png'%filename)
plt.close()

alphalens.plotting.plot_cumulative_returns_by_quantile(active_mean_return_by_q_daily,period=1)
plt.savefig('%s/Active_cumulative_returns_period=1.png'%filename)
plt.close()
alphalens.plotting.plot_cumulative_returns_by_quantile(mean_return_by_q_daily,period=1)
plt.savefig('%s/cumulative_returns_period=1.png'%filename)
plt.close()




## 因子值作为权重构建组合
#ls_factor_returns = alphalens.performance.factor_returns(factor_data,long_short=False)
#ls_factor_cum_returns = ls_factor_returns.add(1).cumprod()
#ls_factor_cum_returns.plot()


################################################################################
def ret_dacay_bar(predictive_factor, Price, lag_range,benchmark):
    '''
    因子lag几天 比较每个lag因子 每组平均日收益率
    '''
    group = []
    group2 = []
    for i in lag_range:
        predictive_factor1 = predictive_factor.shift(i)

        predictive_factor1 = predictive_factor1.stack()
        predictive_factor1.index = predictive_factor1.index.set_names(['date', 'asset'])

        factor_data1 = alphalens.utils.get_clean_factor_and_forward_returns(predictive_factor1,
                                                                           Price,
                                                                           quantiles=10,
                                                                           periods=(1,))
        mean_return_by_q_shift1, _ = alphalens.performance.mean_return_by_quantile(factor_data1, by_group=False,demeaned=False, by_date=True)
        mean_return_by_5 = mean_return_by_q_shift1.loc[(mean_return_by_q_shift1.index.get_level_values(u'factor_quantile') == 5)]

        active_ret = pd.concat([mean_return_by_5, benchmark], join='inner', axis=1)
        active_ret[1] = active_ret[1] - active_ret[benchmark.columns.tolist()[0]]
        active_ret = active_ret[1]
        ir = (12 * active_ret.mean()) / (active_ret.std() * np.sqrt(12))

        group.append(active_ret.mean())
        group2.append(ir)

    df = pd.DataFrame()
    for j in lag_range:
        df['lag_%s'%j] = [group[j]]

    df =df.T
    df.plot(kind='bar')

    plt.savefig('%s/factor_lag_mean_return_by_quantile.png'%filename)
    plt.close()


    df2 = pd.DataFrame()
    for j in lag_range:
        df2['lag_%s'%j] = [group2[j]]

    df2 =df2.T
    df2.plot(kind='bar')

    plt.savefig('%s/factor_lag_ir_by_quantile.png'%filename)
    plt.close()


predictive_factor = df_factor
lag_range = range(5)
ret_dacay_bar(predictive_factor, df_close, lag_range,test5)


# ###############################################################################
# def plot_top_bottom_decay(predictive_factor, Price, lag_range):
#     '''
#     最好的组减去最差的组的累计收益率 因子lag
#     '''
#     df = pd.DataFrame()
#     for i in lag_range:
#         predictive_factor1 = predictive_factor.shift(i)
#         predictive_factor1 = predictive_factor1.stack()
#         predictive_factor1.index = predictive_factor1.index.set_names(['date', 'asset'])
#         factor_data1 = alphalens.utils.get_clean_factor_and_forward_returns(predictive_factor1,
#                                                                            Price,
#                                                                            quantiles=5,
#                                                                            periods=(1,)
#
#         mean_return_by_q_daily1, std_err = alphalens.performance.mean_return_by_quantile(factor_data1, by_date=True,demeaned=False)
#         ## 等权做多最好分位，做空最差分位
#         quant_return_spread, std_err_spread = alphalens.performance.compute_mean_returns_spread(mean_return_by_q_daily1,
#                                                                                                 upper_quant=5,
#                                                                                                 lower_quant=1,
#                                                                                                 std_err=std_err)
#         #alphalens.plotting.plot_mean_quantile_returns_spread_time_series(quant_return_spread, std_err_spread)
#         df['lag_%s'%i] = quant_return_spread[1]
#
#     (df.add(1).cumprod()).plot(colormap='Set1')  # colormap='Paired'
#     plt.savefig('%s/factor_lag_cum_returns_spread.png'%filename)
#     plt.close()
#     return df
#
# predictive_factor = df_factor
# lag_range = range(10)
# df = plot_top_bottom_decay(predictive_factor, df_close, lag_range)


ic = alphalens.performance.factor_information_coefficient(factor_data)
((ic[1])).cumsum().plot(title = u'IC累加曲线',figsize=(12, 10))
plt.savefig('%s/cumsum_IC.png'%filename)
plt.close()

#alphalens.plotting.plot_ic_ts(ic);

#####累计ic
#quantile_factor = factor_data['factor_quantile']
#ic_period = 1
#df = pd.DataFrame()
#for i in range(1, quantile_factor.max() + 1):
#
#    factor_data_group1 = factor_data[factor_data['factor_quantile']==i]
#    ic_group1 = alphalens.performance.factor_information_coefficient(factor_data_group1)
#    df['group%s'%i] = ic_group1[ic_period]
#
#df.cumsum().plot()
#plt.savefig('%s/cumsum_IC.png'%filename)
#plt.close

###### 换手率
quantile_factor = factor_data['factor_quantile']
turnover_period = 1

quantile_turnover = pd.concat([alphalens.performance.quantile_turnover(quantile_factor, q, turnover_period)
                               for q in range(1, quantile_factor.max() + 1)], axis=1)

alphalens.plotting.plot_top_bottom_quantile_turnover(quantile_turnover, turnover_period)
plt.savefig('%s/turnover%s.png'%(filename,turnover_period))
plt.close()

print '##################################'
print 'turnover:'
print quantile_turnover.mean()



###############################################################################

'''
predictive_factor = score_df

group = []
lag_range = range(9)
df = pd.DataFrame()

for i in lag_range:
    predictive_factor1 = predictive_factor.shift(i)
    predictive_factor1 = predictive_factor1.stack()
    predictive_factor1.index = predictive_factor1.index.set_names(['date', 'asset'])
    factor_data1 = alphalens.utils.get_clean_factor_and_forward_returns(predictive_factor1, 
                                                                       Open, 
                                                                       quantiles=5)
    
    factor_data_group1 = factor_data1[factor_data1['factor_quantile']==1]
    ic_group1 = alphalens.performance.factor_information_coefficient(factor_data_group1)

    df['lag_%s'%i] = ic_group1.mean()

df =df.T
df.plot(kind='bar')
'''