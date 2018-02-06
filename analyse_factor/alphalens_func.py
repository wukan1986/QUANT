# -*- coding: utf-8 -*-


import alphalens
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
from pylab import mpl   #画图显示中文
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False



def ret_dacay_bar(predictive_factor, Price, lag_range,benchmark, filename):
    '''
    因子lag几期 比较每个lag因子 每组平均日收益率
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

def plot(filename, df_factor, df_close, df_benchmark, benchmark_group,col_index):

    factor_init = df_factor.stack()
    #alphalens.tears.create_factor_tear_sheet(factor_init, close)
    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(factor_init, df_close,periods=(1,),quantiles=10)

    #ic heatmap
    mean_monthly_ic = alphalens.performance.mean_information_coefficient(factor_data, by_time='M')
    alphalens.plotting.plot_monthly_ic_heatmap(mean_monthly_ic)
    plt.savefig('%s/IC_heatmap.png'%filename)
    plt.close()

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


    predictive_factor = df_factor
    lag_range = range(5)
    ret_dacay_bar(predictive_factor, df_close, lag_range,benchmark_group, filename)


    ic = alphalens.performance.factor_information_coefficient(factor_data)
    ((ic[1])).cumsum().plot(title = u'IC累加曲线',figsize=(12, 10))
    plt.savefig('%s/cumsum_IC.png'%filename)
    plt.close()

    alphalens.plotting.plot_ic_ts(ic);
    plt.savefig('%s/TimeSeries_IC.png'%filename)
    plt.close()

    alphalens.plotting.plot_ic_hist(ic)
    plt.savefig('%s/Bar_IC.png'%filename)
    plt.close()

    alphalens.plotting.plot_ic_qq(ic)
    plt.savefig('%s/QQ_IC.png'%filename)
    plt.close()

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