# -*- coding: utf-8 -*-

import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml
import os


def plot_heatmap(factor, dirpath, outputpath):
    df = pd.read_csv('%s/%sPeriod_Summary.csv'%(dirpath, factor[0]), usecols=['Period'])

    for factor_name in factor:
        df_summary = pd.read_csv('%s/%sPeriod_Summary.csv'%(dirpath, factor_name), usecols=['Period','Active Return'])
        df_summary.columns = ['Period',factor_name]
        df = df.merge(df_summary,on = 'Period')
    df_corr = df.corr()

    f, ax1 = plt.subplots(figsize=(12,8))
    sns.heatmap(df_corr, annot=True, cmap='coolwarm', ax=ax1, vmax=1, vmin = -1)
    f.savefig('%s/sns_heatmap_annot.jpg'%outputpath)
    plt.close()
    # f.savefig('sns_heatmap_annot.jpg')

if __name__ == '__main__':
    with open('E:/QUANT/R_code/config.yaml') as f:
        temp = yaml.load(f.read())
    lag = int(temp['lag'])

    dirpath = temp['dirpath']
    outputpath = temp['outputpath']
    market_path = temp['market_path']

    if not os.path.exists(outputpath):
        os.makedirs(outputpath)
    if not os.path.exists('%s/summary_excel' % dirpath):
        os.makedirs('%s/summary_excel' % dirpath)

    factor = temp['factor_name']
    factor = factor.split(',')

    plot_heatmap(factor, dirpath, outputpath)
