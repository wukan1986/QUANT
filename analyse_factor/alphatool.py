# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

matplotlib.style.use('ggplot')
from pylab import mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


def caculate_ret(group, date_index, df_ret, weight):
    # try:
    w_ret = (weight.loc[date_index, group] * df_ret.ix[date_index, group]).sum() / weight.loc[date_index, group].sum()
    # except:
    #     w_ret = 0
    return w_ret


def get_cum_ret(df_factor, df_ret, df_index, weight, factor_name, n_quantile, pct_quantiles, outputpath):
    col = []
    for i in range(n_quantile):
        col.append('group' + str(i + 1))

    df = pd.DataFrame([], columns=col)
    for i, row in df_factor.iterrows():
        score = row.dropna()
        if not score.empty:
            for k in range(n_quantile):
                groupi = 'group' + str(k + 1)
                down = score.quantile(pct_quantiles * k)
                up = score.quantile(pct_quantiles * (k + 1))
                stock_code = list(score[(score <= up) & (score >= down)].index)
                df.loc[i, groupi] = caculate_ret(stock_code, i, df_ret, weight)
        else:
            df.ix[i, :] = np.zeros(n_quantile)

    df_index.columns = ['Benchmark']
    df_summary = pd.concat([df, df_index], join='inner', axis=1)

    b = (df_summary + 1).cumprod()
    b.to_csv('tmp.csv')
    b.plot(title=u'%s_累计收益率' % factor_name, figsize=(12, 8),cmap = 'jet')
    plt.savefig(u'%s/%s_累计收益率' % (outputpath, factor_name))
    plt.close()

    df2 = df_summary.copy()
    for k in range(n_quantile):
        groupi = 'group' + str(k + 1)
        df2[groupi] = df2[groupi] - df2['Benchmark']
    del df2['Benchmark']
    bb = (df2 + 1).cumprod()
    bb.plot(title=u'%s_累计超额收益率' % factor_name, figsize=(12, 8),cmap = 'jet')
    plt.savefig(u'%s/%s_超额累计收益率' % (outputpath, factor_name))
    plt.close()

def get_decay(df_factor_raw, df_ret, df_index, weight, lag_num, factor_name, n_quantile, pct_quantiles, outputpath):
    # df_factor_raw  = df_factor.copy()

    col = []
    for i in range(n_quantile):
        col.append('group' + str(i + 1))
    df_decay = pd.DataFrame([], index=col)
    df_ir = pd.DataFrame([], index=col)
    for lag in range(lag_num):
        df_factor1 = df_factor_raw.shift(lag)
        df_factor1 = df_factor1.iloc[lag:]
        df = pd.DataFrame([], columns=col)
        for i, row in df_factor1.iterrows():
            score = row.dropna()
            if not score.empty:
                for k in range(n_quantile):
                    groupi = 'group' + str(k + 1)
                    down = score.quantile(pct_quantiles * k)
                    up = score.quantile(pct_quantiles * (k + 1))
                    stock_code = list(score[(score <= up) & (score >= down)].index)
                    df.loc[i, groupi] = caculate_ret(stock_code, i, df_ret, weight)
            else:
                df.ix[i, :] = np.zeros(n_quantile)

        df_index.columns = ['Benchmark']
        df_summary = pd.concat([df, df_index], join='inner', axis=1)
        df2 = df_summary.copy()
        for k in range(n_quantile):
            groupi = 'group' + str(k + 1)
            df2[groupi] = df2[groupi] - df2['Benchmark']
        del df2['Benchmark']

        df_decay['lag_%s' % lag] = df2.mean().values

        ir = (12 * df2.mean()) / (df2.std() * np.sqrt(12))
        df_ir['lag_%s' % lag] = ir.values
    df_decay = df_decay.T
    df_ir = df_ir.T

    df_decay.plot(kind='bar', title=u'%s_平均超额收益率衰减' % factor_name, figsize=(12, 8),cmap = 'tab10')
    plt.savefig(u'%s/%s_平均超额收益率衰减' % (outputpath, factor_name))
    plt.close()
    df_ir.plot(kind='bar', title=u'%sIR_衰减' % factor_name, figsize=(12, 8),cmap = 'tab10')
    plt.savefig(u'%s/%s_IR_衰减' % (outputpath, factor_name))
    plt.close()


def get_eventdrive(df_factor, daily, df_ret_daily, df_index_daily, date_range, factor_name, n_quantile, pct_quantiles,
                   outputpath):
    col = []
    for i in range(n_quantile):
        col.append('group' + str(i + 1))

    group_ret = []
    group_benchmark_ret = []
    for i in range(n_quantile):
        group_ret.append(np.array([[]] * (2 * date_range)))
        group_benchmark_ret.append(np.array([[]] * (2 * date_range)))

    for i, row in df_factor.iterrows():
        score = row.dropna()
        if not score.empty:
            for k in range(n_quantile):
                down = score.quantile(pct_quantiles * k)
                up = score.quantile(pct_quantiles * (k + 1))
                stock_code = list(score[(score <= up) & (score >= down)].index)

                select_range = daily[daily < i][-date_range:].tolist() + daily[daily >= i][:date_range].tolist()
                ret_temp = df_ret_daily.loc[select_range, stock_code].values
                # relative_ret_temp = relative_ret.ix[daily[daily >= i][:date_range], stock_code].values
                benchmark_ret_temp = df_index_daily.loc[select_range, :].values

                group_ret[k] = np.column_stack((group_ret[k], ret_temp))
                # group_relative_ret[k] = np.column_stack((group_relative_ret[k], relative_ret_temp))
                ## benchmark可以放在循环外面的 ！！！ 本来一列 现在5列 也不影响
                group_benchmark_ret[k] = np.column_stack((group_benchmark_ret[k], benchmark_ret_temp))

    group_df = pd.DataFrame(index=range(-date_range, 0) + range(date_range), columns=col)
    group_benchmark_df = pd.DataFrame(index=range(-date_range, 0) + range(date_range), columns=col)
    for i in range(n_quantile):
        group_df[col[i]] = np.nanmean(group_ret[i], axis=1)
        group_benchmark_df[col[i]] = np.nanmean(group_benchmark_ret[i], axis=1)

    group_dfs = (1 + group_df).cumprod(axis=0) / (1 + group_df).cumprod(axis=0).loc[0]
    group_benchmark_dfs = (1 + group_benchmark_df).cumprod(axis=0) / (1 + group_benchmark_df).cumprod(axis=0).loc[0]
    group_relative = group_dfs.sub(group_benchmark_dfs)
    # group_relative.plot()

    ####  画图部分
    fig = plt.figure(figsize=(24, 16))

    ax1 = fig.add_subplot(221)
    df = group_dfs
    for i in df.columns:
        ax1.plot(df.index, df[i], linewidth=3.0)
    ax1.legend(loc='best', fontsize=14)
    ax1.set_title(u'日绝对累计收益_%s' % factor_name, fontsize=20)
    ax1.set_ylabel(u'收益率', fontsize=18)
    ax1.set_xlabel(u'天数', fontsize=18)
    ax1.tick_params(axis='both', which='major', labelsize=20)

    ax2 = fig.add_subplot(222)
    df = group_df
    for i in df.columns:
        ax2.plot(df.index, df[i], linewidth=3.0)
    ax2.legend(loc='best', fontsize=14)
    ax2.set_title(u'日绝对收益_%s' % factor_name, fontsize=20)
    ax2.set_ylabel(u'收益率', fontsize=18)
    ax2.set_xlabel(u'天数', fontsize=18)
    ax2.tick_params(axis='both', which='major', labelsize=20)

    ax3 = fig.add_subplot(223)
    df = group_relative
    for i in df.columns:
        ax3.plot(df.index, df[i], linewidth=3.0)
    ax3.legend(loc='best', fontsize=14)
    ax3.set_title(u'日超额累计收益_%s' % factor_name, fontsize=20)
    ax3.set_ylabel(u'收益率', fontsize=18)
    ax3.set_xlabel(u'天数', fontsize=18)
    ax3.tick_params(axis='both', which='major', labelsize=20)

    ax4 = fig.add_subplot(224)
    df = group_df.sub(group_benchmark_df)
    for i in df.columns:
        ax4.plot(df.index, df[i], linewidth=3.0)
    ax4.legend(loc='best', fontsize=14)
    ax4.set_title(u'日超额收益_%s' % factor_name, fontsize=20)
    ax4.set_ylabel(u'收益率', fontsize=18)
    ax4.set_xlabel(u'天数', fontsize=18)
    ax4.tick_params(axis='both', which='major', labelsize=20)

    plt.savefig(u'%s/%s日收益率分组图_%s.png' % (outputpath, factor_name, date_range))
    plt.clf()
