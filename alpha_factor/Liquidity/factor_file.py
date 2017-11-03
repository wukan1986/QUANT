# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 18:08:15 2017

@author: shiyunchao
"""


import pandas as pd
import os 

dirpath = 'E:/QUANT/alpha_factor/Liquidity/raw_data'
factor_name = ['stoa','stom','stoq','Liquidity'] 


for factor in factor_name:
    print(factor)
    if not os.path.exists(factor):
        os.mkdir(factor)
    path = '%s/%s' % (dirpath, factor)
    filelist = os.listdir('%s/%s' % (dirpath, factor))

    for j in filelist:
        new_name = j.split('_')[0] + '_raw_CN_' + j.split('_')[1]
        df = pd.read_csv(path + '/' + j, header = None)
        df.iloc[:,1] = -df.iloc[:,1] 
    
        df.to_csv('%s/%s'%(factor,new_name),header=None,index=None)
