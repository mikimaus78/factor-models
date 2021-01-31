# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 21:51:17 2020

@author: 64858
"""
from scipy.stats import rankdata
import numpy as np
import pandas as pd

       
def rolling_cor(x1,x2,window=20):
    """
    function: rolling cor of 2 series
    parameters: x1,x2,window
    output:corr
    """
    return x1.rolling(window).corr(other=x2)

def rolling_cov(x1,x2,window=20):
    """
    function: rolling cov of 2 series
    parameters: x1,x2,window
    output:cov
    """
    return x1.rolling(window).cov(other=x2)

def rolling_skew(x1,window=20):
    """
    function: rolling cov of 2 series
    parameters: x1,window
    output:skew
    """
    return x1.rolling(window).skew()

def rolling_rank(x1):
    """
    function: rank of a series
    parameters: x1
    output:rankdata
    """
    return rankdata(x1)[-1]

def ts_rank(x1,window=20):
    """
    function: timeseries rank of 1 series
    parameters: x1,window
    output:ts_rolling_rank
    """
    return x1.rolling(window).apply(rolling_rank)

def ts_min(x1,window=20):
    """
    function:  min num of 1 series over a window period
    parameters: x1,window
    output:ts_min
    """
    return x1.rolling(window).min()

def ts_max(x1,window=20):
    """
    function:  max num of 1 series over a window period
    parameters: x1,window
    output:ts_max
    """
    return x1.rolling(window).max()

def delta(x1,period=5):
    """
    function:  diff of 1 series over period on timeseries
    parameters: x1,window
    output:delta
    """
    return x1.diff(period)

def delay(x1,period=5):
    """
    function:  diff of 1 series over period on timeseries
    parameters: x1,period
    output:delta
    """
    return x1.shift(period)

def ts_argmax(x1,window=20):
    """
    function:  max timedelta of 1 series over a window period
    parameters: x1,window
    output:ts_argmax
    """
    return x1.rolling(window).apply(np.argmax) + 1 

def ts_argmin(x1,window=20):
    """
    function:  min timedelta of 1 series over a window period
    parameters: x1,window
    output:ts_argmin
    """
    return x1.rolling(window).apply(np.argmin) + 1

def linear_weight(window=20):
    """
    function:  a list of lineardecay_weight like [0.1,0.2,0.3,0.4]
    parameters: window
    output:linear_weight
    """        
    return  [2*i/window/(window+1) for i in range(1,window+1)]

def decay_shift(x1,window=20):
    """
    function:  a list of seriesdecay like [a1.shift(1),a2.shift(1),a3.shift(1)]
    parameters: x1,window
    output:linear_weight
    """ 
    return [x1.shift(i) for i in range(0,window)]

def decaylinear(x1,window=20):
    """
    function: linear_weighted of a DataFrame
    parameters: x1,window
    output:df_decay
    """
    df_decay = pd.DataFrame(np.zeros(x1.shape),columns =x1.columns,index = x1.index)
    for j in range(0,window):
        buf = linear_weight(window)[j]*decay_shift(x1,window)[window-1-j]
        df_decay = df_decay + buf
    return df_decay

def ten_sigma_filter(x1,n=10):
    """
    function: volatility filter of 1 series
    parameters: x1, n
    output:series_filter
    """
    upline = x1.mean()+n*x1.std() 
    downline = x1.mean()-n*x1.std()
    if downline > x1.min():
        x1[x1<downline] = downline
    if upline < x1.max():
        x1[x1>upline] = upline
    return x1

def quantile_filter(x1,quantile=0.01):
    """
    function: volatility filter of 1 series
    parameters: x1, n
    output:series_filter
    """
    x1[x1.rank(pct=True)<=quantile] = x1.mean()
    x1[x1.rank(pct=True)>=1-quantile] = x1.mean()

    return x1

def zscore(x1):
    """
    function:  normalize of 1 series 
    parameters: x1 
    output:zscore_x1
    """
    return (x1-x1.mean())/x1.std()

def subsets(x1):
    """ 
    function:  find all subsets of a list 
    parameters: x1 
    output:subsets_x1
    """
    output = [[]]
    for i in range(len(x1)):
        for j in range(len(output)):
            output.append(output[j]+[x1[i]])
    output = sorted(output, key = lambda x: len(x1))
    output = output[1:]
    return output

   
        
