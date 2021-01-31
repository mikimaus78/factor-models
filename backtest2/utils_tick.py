# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 17:25:00 2020

@author: 64858
"""
import numpy as np
import pandas as pd
import sys
sys.path.append("/home/ybxu/scripts/")
import os
import copy
import statsmodels.api as sm
from config.root_tick import trading_day_root,const_root,tonglian_stock_trans_root,tonglian_stock_l2_root,tonglian_future_root,lu_path,tonglian_commodity_root,commodity_constract_path 
from config.target_list import index_map,index_list,future_list
from config.fac_list_tick import return_period_list,next_return_dict,next_return_within_1tick_dict
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

                              
def get_trading_years(start_day:int,end_day:int):
    file_path = os.path.join(trading_day_root,f'trading_day.pkl')
    trading_day = pd.read_pickle(file_path)
    trading_day['date'] = trading_day['date'].apply(lambda x: int(x.strftime('%Y%m%d')))
    trading_day = trading_day[(trading_day.date>=start_day)&(trading_day.date<=end_day)].reset_index(drop=True)
    trading_day_list = trading_day.date.tolist()
    trading_year_list = (trading_day.date//10000).unique().tolist()
    return trading_day_list,trading_year_list

def get_trading_daysMonths(start_day:int,end_day:int):
    file_path = os.path.join(trading_day_root,f'trading_day.pkl')
    trading_day = pd.read_pickle(file_path)
    if trading_day.columns != ['date']:
        trading_day.columns = ['date']
    trading_day['date'] = trading_day['date'].apply(lambda x: int(x.strftime('%Y%m%d')))
    trading_day = trading_day[(trading_day.date>=start_day)&(trading_day.date<=end_day)].reset_index(drop=True)
    trading_day_list = trading_day.date.tolist()
    trading_month_list = (trading_day.date//100).unique().tolist()
    return trading_day_list,trading_month_list

def get_index_weight(trade_date:int,index:str):
    index_path = os.path.join(const_root, f'{index_map[index]}')
    index_weight = pd.read_pickle(index_path)[trade_date]   
    index_weight['code'] = index_weight.index
    index_weight['code'] = index_weight['code'].apply(lambda x: str(x)[:6])
    index_weight = index_weight[['code','weight']].reset_index(drop=True)
    index_weight = index_weight.sort_values(by=['code'])    
    index_stock_list = index_weight.code.tolist()
    index_weight.set_index(['code'],inplace=True)
    return index_weight,index_stock_list

def load_stock_trans(trade_date:int,index:str,stock_list):
    tradetime_benchmark = load_tradetime_benchmark()
    trans_path = os.path.join(tonglian_stock_trans_root,f'{index}', f'{str(trade_date)}.csv.gz') 
    trans = pd.read_csv(trans_path)
    trans['ticker'] = trans['ticker'].apply(lambda x: f'{x:06d}')
    trans['date'] = trade_date
    trans['hour'] = trans['time'].apply(lambda x: int(x[11:13]))
    trans['minute'] = trans['time'].apply(lambda x: int(x[14:16]))
    trans['second'] = trans['time'].apply(lambda x: int(x[17:19]))
    trans['microsecond'] = trans['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    trans["ftime"] = trans.hour*10000 + trans.minute*100 + trans.second + (
            trans.microsecond//0.5) * 0.5 + 0.5      
    trans['ftime'] = trans['ftime'].apply(lambda x: (x + 40) if (x%100 == 60) else x)    
    trans['ftime'] = trans['ftime'].apply(lambda x: (x + 4000) if (x/100%100 == 60) else x)                   
    trans = trans[(trans['ftime']>93000)&(trans['ftime']<=113000)|(trans['ftime']>130000)&(trans['ftime']<=145700)]       
    last_price = trans.groupby(["ticker", "ftime"])["close"].last()   
    
    df_last_price = last_price.unstack(level=0).replace(0, np.nan)
    df_last_price = df_last_price.fillna(method="ffill").fillna(method="bfill")
    itrsecList = [i for i in df_last_price.columns if i in stock_list]
    leftList = list(set(stock_list) - set(itrsecList))
    if itrsecList == stock_list:
        df_last_price = df_last_price[stock_list]
    else:
        df_last_price = df_last_price[itrsecList]
        buff = pd.DataFrame(np.zeros((len(df_last_price),len(leftList))),columns = leftList,index = df_last_price.index)
        df_last_price = pd.concat([df_last_price,buff],axis=1)
        df_last_price = df_last_price[stock_list]
    df_last_price['ftime'] = df_last_price.index
    df_last_price.reset_index(drop=True,inplace=True)
    df_last_price = pd.merge(tradetime_benchmark,df_last_price,how='left',on = 'ftime')
    df_last_price.set_index(['ftime'],inplace=True)    
    df_tick_cumret = (df_last_price/df_last_price.iloc[0] - 1).fillna(method='ffill')
    df_tick_ret = df_tick_cumret - df_tick_cumret.shift(1)
    return df_last_price,df_tick_cumret,df_tick_ret

def load_stock_l2(trade_date:int,index:str,stock_list):
    tradetime_benchmark = load_tradetime_benchmark()
    l2_path = os.path.join(tonglian_stock_l2_root,f'{index}', f'{str(trade_date)}.csv.gz') 
    l2 = pd.read_csv(l2_path)
    l2['ticker'] = l2['ticker'].apply(lambda x: f'{x:06d}')
    l2['date'] = trade_date
    l2['hour'] = l2['time'].apply(lambda x: int(x[11:13]))
    l2['minute'] = l2['time'].apply(lambda x: int(x[14:16]))
    l2['second'] = l2['time'].apply(lambda x: int(x[17:19]))
    l2['microsecond'] = l2['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    l2["ftime"] = l2.hour*10000 + l2.minute*100 + l2.second + (
            l2.microsecond//0.5) * 0.5 + 0.5      
    l2['ftime'] = l2['ftime'].apply(lambda x: (x + 40) if (x%100 == 60) else x)    
    l2['ftime'] = l2['ftime'].apply(lambda x: (x + 4000) if (x/100%100 == 60) else x)                   
    l2 = l2[(l2['ftime']>93000)&(l2['ftime']<=113000)|(l2['ftime']>130000)&(l2['ftime']<=145700)]       
    col_name = ['high', 'low', 'last', 'TotalBidVol', 'WAvgBidPri',
       'TotalAskVol', 'WAvgAskPri', 'AskPrice1', 'AskVolume1', 'AskPrice2',
       'AskVolume2', 'AskPrice3', 'AskVolume3', 'AskPrice4', 'AskVolume4',
       'AskPrice5', 'AskVolume5', 'AskPrice6', 'AskVolume6', 'AskPrice7',
       'AskVolume7', 'AskPrice8', 'AskVolume8', 'AskPrice9', 'AskVolume9',
       'AskPrice10', 'AskVolume10', 'BidPrice1', 'BidVolume1', 'BidPrice2',
       'BidVolume2', 'BidPrice3', 'BidVolume3', 'BidPrice4', 'BidVolume4',
       'BidPrice5', 'BidVolume5', 'BidPrice6', 'BidVolume6', 'BidPrice7',
       'BidVolume7', 'BidPrice8', 'BidVolume8', 'BidPrice9', 'BidVolume9',
       'BidPrice10', 'BidVolume10']
    dict_df = {}
    for i in  col_name:
        dict_df[i] = l2.groupby(["ticker", "ftime"])[i].last()  
        dict_df[i] = dict_df[i].unstack(level=0).replace(0, np.nan)

    df_last_price = df_last_price.fillna(method="ffill").fillna(method="bfill")
    itrsecList = [i for i in df_last_price.columns if i in stock_list]
    leftList = list(set(stock_list) - set(itrsecList))
    if itrsecList == stock_list:
        df_last_price = df_last_price[stock_list]
    else:
        df_last_price = df_last_price[itrsecList]
        buff = pd.DataFrame(np.zeros((len(df_last_price),len(leftList))),columns = leftList,index = df_last_price.index)
        df_last_price = pd.concat([df_last_price,buff],axis=1)
        df_last_price = df_last_price[stock_list]
    df_last_price['ftime'] = df_last_price.index
    df_last_price.reset_index(drop=True,inplace=True)
    df_last_price = pd.merge(tradetime_benchmark,df_last_price,how='left',on = 'ftime')
    df_last_price.set_index(['ftime'],inplace=True)    
    df_tick_cumret = (df_last_price/df_last_price.iloc[0] - 1).fillna(method='ffill')
    df_tick_ret = df_tick_cumret - df_tick_cumret.shift(1)
    return df_last_price,df_tick_cumret,df_tick_ret

def load_tradetime_benchmark():
    tradetime_benchmark = pd.read_pickle('/home/ybxu/data/time_benchmark.pkl')
    return tradetime_benchmark

def load_commodity_tradetime_benchmark():
    commodity_time_benchmark = pd.read_pickle('/home/ybxu/data/commodity_time_benchmark.pkl')
    return commodity_time_benchmark


def load_future_realtime(trade_date:int,ft:str):
    tradetime_benchmark = load_tradetime_benchmark()
    future_path = os.path.join(tonglian_future_root, f'{str(trade_date)}.csv.gz')   
    df_future = pd.read_csv(future_path)
    df_future = df_future[df_future['code'] == ft].reset_index(drop=True)
    major_code = df_future.ticker.unique()[0]
    future_major = df_future[df_future.ticker == major_code]
    future_major['date'] = trade_date
    future_major['hour'] = future_major['time'].apply(lambda x: int(x[11:13]))
    future_major['minute'] = future_major['time'].apply(lambda x: int(x[14:16]))
    future_major['second'] = future_major['time'].apply(lambda x: int(x[17:19]))
    future_major['microsecond'] = future_major['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    future_major["ftime"] = future_major.hour*10000 + future_major.minute*100 + future_major.second + (
            future_major.microsecond//0.5) * 0.5 + 0.5      
    future_major['ftime'] = future_major['ftime'].apply(lambda x: (x + 40) if (x%100 == 60) else x)   
    future_major['ftime'] = future_major['ftime'].apply(lambda x: (x + 4000) if (x/100%100 == 60) else x)                   
    future_major = future_major[(future_major['ftime']>93000)&(future_major['ftime']<=113000)|(future_major['ftime']>130000)&(future_major['ftime']<=145700)]
    major_columns_maps = {'ftime':'ftime','ask_price_1':f'{ft}_major_ask_price_1','bid_price_1':f'{ft}_major_bid_price_1',\
                          'ask_volume_1':f'{ft}_major_ask_volume_1','bid_volume_1':f'{ft}_major_bid_volume_1',
                          'total_volume':f'{ft}_major_total_volume','total_amount':f'{ft}_major_total_amount'}                         
    future_major = future_major[major_columns_maps.keys()].rename(columns=major_columns_maps)
    future_major[f'{ft}_major_midPrc'] = (future_major[f'{ft}_major_ask_price_1'] + future_major[f'{ft}_major_bid_price_1'])/2
    if (ft == 'IF')| (ft == 'IH'):
        future_major[f'{ft}_major_tick_vwap'] = future_major[f'{ft}_major_total_amount'] / future_major[f'{ft}_major_total_volume']/300
    if ft == 'IC':
        future_major[f'{ft}_major_tick_vwap'] = future_major[f'{ft}_major_total_amount'] / future_major[f'{ft}_major_total_volume']/200
    future_major = pd.merge(tradetime_benchmark,future_major,how='left',on = 'ftime')
    future_major.set_index(['ftime'],inplace=True)
    future_major = future_major.fillna(method='ffill').fillna(method='bfill')
    return future_major

def load_future(trade_date:int,ft:str):
    tradetime_benchmark = load_tradetime_benchmark()
    future_path = os.path.join(tonglian_future_root, f'{str(trade_date)}.csv.gz')   
    df_future = pd.read_csv(future_path)
    df_future = df_future[df_future['code'] == ft].reset_index(drop=True)
    major_code = df_future.ticker.unique()[0]
    sub_code = df_future.ticker.unique()[2]
    future_major = df_future[df_future.ticker == major_code]
    future_major['date'] = trade_date
    future_major['hour'] = future_major['time'].apply(lambda x: int(x[11:13]))
    future_major['minute'] = future_major['time'].apply(lambda x: int(x[14:16]))
    future_major['second'] = future_major['time'].apply(lambda x: int(x[17:19]))
    future_major['microsecond'] = future_major['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    future_major["ftime"] = future_major.hour*10000 + future_major.minute*100 + future_major.second + (
            future_major.microsecond//0.5) * 0.5 + 0.5      
    future_major['ftime'] = future_major['ftime'].apply(lambda x: (x + 40) if (x%100 == 60) else x)           
    future_major = future_major[(future_major['ftime']>=93000)&(future_major['ftime']<=150000)]
    major_columns_maps = {'ftime':'ftime', 'last_price':f'{ft}_major_last_price',\
                          'total_volume':f'{ft}_major_total_volume', 'total_amount':f'{ft}_major_total_amount',
                          'open_interest':f'{ft}_major_open_interest', 'open':f'{ft}_major_open', 
                          'close':f'{ft}_major_close', 'high':f'{ft}_major_high', 'low':f'{ft}_major_low', 
                          'ask_price_1':f'{ft}_major_ask_price_1','bid_price_1':f'{ft}_major_bid_price_1', 
                          'ask_volume_1':f'{ft}_major_ask_volume_1', 'bid_volume_1':f'{ft}_major_bid_volume_1'}                         
    future_major = future_major[major_columns_maps.keys()].rename(columns=major_columns_maps)
                     
    future_major[f'{ft}_major_midPrc'] = (future_major[f'{ft}_major_ask_price_1'] + future_major[f'{ft}_major_bid_price_1'])/2
    future_major[f'{ft}_major_1tick_cumreturn'] = (future_major[f'{ft}_major_midPrc']/future_major[f'{ft}_major_midPrc'].iloc[0]-1).fillna(method='ffill')
    future_major[f'{ft}_major_1tick_pnl'] = future_major[f'{ft}_major_1tick_cumreturn'] - future_major[f'{ft}_major_1tick_cumreturn'].shift(1)
    future_major[f'{ft}_major_1tick_vol'] = future_major[f'{ft}_major_total_volume'] - future_major[f'{ft}_major_total_volume'].shift(1)
    future_major[f'{ft}_major_1tick_amt'] = future_major[f'{ft}_major_total_amount'] - future_major[f'{ft}_major_total_amount'].shift(1)
    if (ft == 'IF')| (ft == 'IH'):
        future_major[f'{ft}_major_tick_vwap'] = future_major[f'{ft}_major_total_amount'] / future_major[f'{ft}_major_total_volume']/300
    if ft == 'IC':
        future_major[f'{ft}_major_tick_vwap'] = future_major[f'{ft}_major_total_amount'] / future_major[f'{ft}_major_total_volume']/200

    future_major = pd.merge(tradetime_benchmark,future_major,how='left',on = 'ftime')
    future_major.set_index(['ftime'],inplace=True)
    future_major = future_major.fillna(method='ffill').fillna(method='bfill')
   
    
    future_sub = df_future[df_future.ticker == sub_code]
    future_sub['date'] = trade_date
    future_sub['hour'] = future_sub['time'].apply(lambda x: int(x[11:13]))
    future_sub['minute'] = future_sub['time'].apply(lambda x: int(x[14:16]))
    future_sub['second'] = future_sub['time'].apply(lambda x: int(x[17:19]))
    future_sub['microsecond'] = future_sub['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    future_sub["ftime"] = future_sub.hour*10000 + future_sub.minute*100 + future_sub.second + (
            future_sub.microsecond//0.5) * 0.5 + 0.5      
    future_sub['ftime'] = future_sub['ftime'].apply(lambda x: (x + 40) if (x%100 == 60) else x)           
    future_sub = future_sub[(future_sub['ftime']>=93000)&(future_sub['ftime']<=150000)]
    sub_columns_maps = {'ftime':'ftime', 'last_price':f'{ft}_sub_last_price',\
                        'total_volume':f'{ft}_sub_total_volume', 'total_amount':f'{ft}_sub_total_amount',
                        'open_interest':f'{ft}_sub_open_interest', 'open':f'{ft}_sub_open', 
                        'close':f'{ft}_sub_close', 'high':f'{ft}_sub_high', 'low':f'{ft}_sub_low', 
                        'ask_price_1':f'{ft}_sub_ask_price_1','bid_price_1':f'{ft}_sub_bid_price_1', 
                        'ask_volume_1':f'{ft}_sub_ask_volume_1', 'bid_volume_1':f'{ft}_sub_bid_volume_1'}                         
    future_sub = future_sub[sub_columns_maps.keys()].rename(columns=sub_columns_maps)
                     
    future_sub[f'{ft}_sub_midPrc'] = (future_sub[f'{ft}_sub_ask_price_1'] + future_sub[f'{ft}_sub_bid_price_1'])/2
    future_sub[f'{ft}_sub_1tick_cumreturn'] = (future_sub[f'{ft}_sub_midPrc']/future_sub[f'{ft}_sub_midPrc'].iloc[0]-1).fillna(method='ffill')
    future_sub[f'{ft}_sub_1tick_pnl'] = future_sub[f'{ft}_sub_1tick_cumreturn'] - future_sub[f'{ft}_sub_1tick_cumreturn'].shift(1)
    future_sub[f'{ft}_sub_1tick_vol'] = future_sub[f'{ft}_sub_total_volume'] - future_sub[f'{ft}_sub_total_volume'].shift(1)
    future_sub[f'{ft}_sub_1tick_amt'] = future_sub[f'{ft}_sub_total_amount'] - future_sub[f'{ft}_sub_total_amount'].shift(1)
    future_sub = pd.merge(tradetime_benchmark,future_sub,how='left',on = 'ftime')
    future_sub.set_index(['ftime'],inplace=True)
    future_sub = future_sub.fillna(method='ffill').fillna(method='bfill')

    return future_major,future_sub

def load_commodity(trade_date:int,cmmd:str):
    commodity_tradetime_benchmark = load_commodity_tradetime_benchmark()
    commodity_path = os.path.join(tonglian_commodity_root, f'{str(trade_date)}.csv.gz')   
    main_constract_list_path = os.path.join(commodity_constract_path,f'{cmmd}.csv')
    main_constract_list = pd.read_csv(main_constract_list_path,index_col = 0)
    df_commodity = pd.read_csv(commodity_path)
    df_commodity = df_commodity[df_commodity['code'] == cmmd].reset_index(drop=True)
    major_code = main_constract_list.loc[trade_date]["main"]
    sub_code = main_constract_list.loc[trade_date]["sub"]
    commodity_major = df_commodity[df_commodity.ticker == major_code]
    commodity_major['date'] = trade_date
    commodity_major['hour'] = commodity_major['time'].apply(lambda x: int(x[11:13]))
    commodity_major['minute'] = commodity_major['time'].apply(lambda x: int(x[14:16]))
    commodity_major['second'] = commodity_major['time'].apply(lambda x: int(x[17:19]))
    commodity_major['microsecond'] = commodity_major['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    commodity_major["ftime"] = commodity_major.hour*10000 + commodity_major.minute*100 + commodity_major.second + (
            commodity_major.microsecond//0.5) * 0.5 + 0.5      
    major_columns_maps = {'ticker':'ticker','ftime':'ftime', 'last_price':f'{cmmd}_major_last_price',\
                          'total_volume':f'{cmmd}_major_total_volume', 'total_amount':f'{cmmd}_major_total_amount',
                          'open_interest':f'{cmmd}_major_open_interest', 'open':f'{cmmd}_major_open', 
                          'close':f'{cmmd}_major_close', 'high':f'{cmmd}_major_high', 'low':f'{cmmd}_major_low', 
                          'ask_price_1':f'{cmmd}_major_ask_price_1','bid_price_1':f'{cmmd}_major_bid_price_1', 
                          'ask_volume_1':f'{cmmd}_major_ask_volume_1', 'bid_volume_1':f'{cmmd}_major_bid_volume_1'}                         
    commodity_major = commodity_major[major_columns_maps.keys()].rename(columns=major_columns_maps)
                     
    commodity_major[f'{cmmd}_major_spread'] = (commodity_major[f'{cmmd}_major_ask_price_1'] - commodity_major[f'{cmmd}_major_bid_price_1'])    
    commodity_major[f'{cmmd}_major_midPrc'] = (commodity_major[f'{cmmd}_major_ask_price_1'] + commodity_major[f'{cmmd}_major_bid_price_1'])/2
    commodity_major[f'{cmmd}_major_1tick_cumreturn'] = (commodity_major[f'{cmmd}_major_midPrc']/commodity_major[f'{cmmd}_major_midPrc'].iloc[0]-1).fillna(method='ffill')
    commodity_major[f'{cmmd}_major_1tick_pnl'] = commodity_major[f'{cmmd}_major_1tick_cumreturn'] - commodity_major[f'{cmmd}_major_1tick_cumreturn'].shift(1)
    commodity_major[f'{cmmd}_major_1tick_vol'] = commodity_major[f'{cmmd}_major_total_volume'] - commodity_major[f'{cmmd}_major_total_volume'].shift(1)
    commodity_major[f'{cmmd}_major_1tick_amt'] = commodity_major[f'{cmmd}_major_total_amount'] - commodity_major[f'{cmmd}_major_total_amount'].shift(1)
    commodity_major = pd.merge(commodity_tradetime_benchmark,commodity_major,how='left',on = 'ftime')
    commodity_major.set_index(['ftime'],inplace=True)
    commodity_major = commodity_major.fillna(method='ffill').fillna(method='bfill')


    commodity_sub = df_commodity[df_commodity.ticker == sub_code]
    commodity_sub['date'] = trade_date
    commodity_sub['hour'] = commodity_sub['time'].apply(lambda x: int(x[11:13]))
    commodity_sub['minute'] = commodity_sub['time'].apply(lambda x: int(x[14:16]))
    commodity_sub['second'] = commodity_sub['time'].apply(lambda x: int(x[17:19]))
    commodity_sub['microsecond'] = commodity_sub['time'].apply(lambda x: int(x[20:])/1e3 if len(x) == 23 else 0)
    commodity_sub["ftime"] = commodity_sub.hour*10000 + commodity_sub.minute*100 + commodity_sub.second + (
            commodity_sub.microsecond//0.5) * 0.5 + 0.5      
    commodity_sub['ftime'] = commodity_sub['ftime'].apply(lambda x: (x + 40) if (x%100 == 60) else x)           
    sub_columns_maps = {'ticker':'ticker','ftime':'ftime', 'last_price':f'{cmmd}_sub_last_price',\
                        'total_volume':f'{cmmd}_sub_total_volume', 'total_amount':f'{cmmd}_sub_total_amount',
                        'open_interest':f'{cmmd}_sub_open_interest', 'open':f'{cmmd}_sub_open', 
                        'close':f'{cmmd}_sub_close', 'high':f'{cmmd}_sub_high', 'low':f'{cmmd}_sub_low', 
                        'ask_price_1':f'{cmmd}_sub_ask_price_1','bid_price_1':f'{cmmd}_sub_bid_price_1', 
                        'ask_volume_1':f'{cmmd}_sub_ask_volume_1', 'bid_volume_1':f'{cmmd}_sub_bid_volume_1'}                         
    commodity_sub = commodity_sub[sub_columns_maps.keys()].rename(columns=sub_columns_maps)
                     
    commodity_sub[f'{cmmd}_sub_spread'] = (commodity_sub[f'{cmmd}_sub_ask_price_1'] - commodity_sub[f'{cmmd}_sub_bid_price_1'])    
    commodity_sub[f'{cmmd}_sub_midPrc'] = (commodity_sub[f'{cmmd}_sub_ask_price_1'] + commodity_sub[f'{cmmd}_sub_bid_price_1'])/2
    commodity_sub[f'{cmmd}_sub_1tick_cumreturn'] = (commodity_sub[f'{cmmd}_sub_midPrc']/commodity_sub[f'{cmmd}_sub_midPrc'].iloc[0]-1).fillna(method='ffill')
    commodity_sub[f'{cmmd}_sub_1tick_pnl'] = commodity_sub[f'{cmmd}_sub_1tick_cumreturn'] - commodity_sub[f'{cmmd}_sub_1tick_cumreturn'].shift(1)
    commodity_sub[f'{cmmd}_sub_1tick_vol'] = commodity_sub[f'{cmmd}_sub_total_volume'] - commodity_sub[f'{cmmd}_sub_total_volume'].shift(1)
    commodity_sub[f'{cmmd}_sub_1tick_amt'] = commodity_sub[f'{cmmd}_sub_total_amount'] - commodity_sub[f'{cmmd}_sub_total_amount'].shift(1)
    commodity_sub = pd.merge(commodity_tradetime_benchmark,commodity_sub,how='left',on = 'ftime')
    commodity_sub.set_index(['ftime'],inplace=True)
    commodity_sub = commodity_sub.fillna(method='ffill').fillna(method='bfill')

    return commodity_major,commodity_sub

def load_lu(trade_date:int):
    lu_index = ['300','500']
    lu = {}
    df_lu = pd.DataFrame()
    for idx in lu_index:
        signal_path = os.path.join(lu_path, f'510{idx}.{int(trade_date//1e4)}-{int(trade_date%1e4//1e2):02d}-{int(trade_date%1e2):02d}.csv')
        lu[idx] = pd.read_csv(signal_path)       
        lu[idx]['ftime'] = lu[idx]['time'].apply(lambda x: float(x/1e3))
        lu[idx].set_index(['ftime'],inplace=True)
        lu[idx].drop(['time'],axis=1,inplace=True)       
        signal_columns_maps={'c1h':f'{idx}_c1h','p1h':f'{idx}_p1h','p2h':f'{idx}_p2h'}
        lu[idx] = lu[idx][signal_columns_maps.keys()].rename(columns=signal_columns_maps)
        if len(df_lu) == 0:
            df_lu = lu[idx]
        else:
            df_lu = pd.concat([df_lu,lu[idx]],axis=1)
    print(trade_date)  
    return df_lu 

def signalfactor_monthly_test(df,month:int,signal_fac_name:str,next_ntick_return:str,method="OLS"):
    """
    function: signalfactor_regression
    parameters:df,month,signal_fac_name,next_ntick_return,method
    output:res['params','tvalues','rsquared']
    """
    df = df[[signal_fac_name,next_ntick_return]]
    df = df.dropna().replace([np.inf,-np.inf],0)
    res = pd.DataFrame(np.zeros((1,3)))
    columns = ['coef_'+signal_fac_name]
    columns.append('tvalues_'+signal_fac_name)
    columns.append('Rsquared')
    
    res.columns = columns
    y = df[next_ntick_return]
    x = df[signal_fac_name]
    x = sm.add_constant(x)
    if method == "OLS" :
        LinearReg = sm.OLS(y,x).fit() 
    if method == "WLS" :        
        LinearReg = sm.WLS(y,x,weights=np.abs(y)+1e-7).fit() 
    res.iloc[0,0] = float(LinearReg.params[1])
    res.iloc[0,1] = float(LinearReg.tvalues[1])
    res.iloc[0,2] = float(LinearReg.rsquared)
    res.index = [month]
    return res
   
def multifactor_monthly_test(df,month:int,multi_fac_list:str,next_ntick_return:str,firtick = False,method="OLS"):
    """
    function: multifactor_regression
    parameters:df,month,muti_fac_list,next_ntick_return,method
    output:res['params','tvalues','rsquared']
    """
    df_monthly = copy.deepcopy(df)
    if firtick == False:
        df_monthly[next_ntick_return] = df_monthly[next_ntick_return].shift(-1)
    reg_columns = copy.deepcopy(multi_fac_list)
    reg_columns.append(next_ntick_return)
    df_monthly = df_monthly[reg_columns]
    df_monthly = df_monthly.dropna().replace([np.inf,-np.inf],0)
    res = pd.DataFrame(np.zeros((1,len(multi_fac_list)*2+1)))
    columns = copy.deepcopy(multi_fac_list)
    for fac in multi_fac_list:
        columns.append('tvalues_'+fac) 
    columns.append('Rsquared')

    res.columns = columns  
    y = df_monthly[next_ntick_return]
    x = df_monthly[multi_fac_list]
    x = sm.add_constant(x)
    if method == "OLS" :
        LinearReg = sm.OLS(y,x).fit() 
    if method == "WLS" :        
        LinearReg = sm.WLS(y,x,weights=np.abs(y)+1e-7).fit() 
    for j in range(0,len(columns)):
        if j < len(multi_fac_list):
            res.iloc[0,j] = float(LinearReg.params[j+1])
        elif j < len(multi_fac_list)*2:
            res.iloc[0,j] = float(LinearReg.tvalues[j+1-len(multi_fac_list)])
        else:
            res.iloc[0,j] = float(LinearReg.rsquared)
    res.index = [month]       
    return res

def signalfactor_monthly_parameter_decay_test(df,month:int,signal_fac_name:str,retname:str,N=100,method="OLS"):
    """
    function: signalfactor_regression with next n period return(n=1,2,3..,60) to test best parameter
    parameters:df,retname,month,signal_fac_name,N,method
    output:res['params','tvalues','rsquared']
    """
    df = df[[signal_fac_name,retname]].dropna().replace([np.inf,-np.inf],0)
    res = pd.DataFrame(np.zeros((N,1)))
    res.columns = [str(month)+'_tvalues']
    
    for decay in range(0,N):
        df[retname+'_decay'] = df[retname].shift(-(decay+1))
        df = df.dropna()
        y = df[retname+'_decay']
        x = df[signal_fac_name]
        x = sm.add_constant(x)
        if method == "OLS" :
            LinearReg = sm.OLS(y,x).fit() 
        if method == "WLS" :        
            LinearReg = sm.WLS(y,x,weights=np.abs(y)+1e-7).fit() 
        res.iloc[decay,0] = float(LinearReg.tvalues[1])
        print(decay)
    return res

 
def cal_daily_ic(df,date:int,signal_fac_name:str,next_ntick_return:str,method='values'):
    """
    function:cal_daily_ic
    parameters: df,date,signal_fac_name,next_ntick_return,method('values'--ic;'rank'--rank_ic)
    output: daily_ic_df
    """ 
    ic = pd.DataFrame(np.zeros((1,1)),columns=['ic_daily'],index = [date])
    ic.iloc[0,0] = df[signal_fac_name].corr(df[next_ntick_return])
    if method == 'values':
        ic.iloc[0,0] = df[signal_fac_name].corr(df[next_ntick_return])
    elif method == 'rank':
        ic.iloc[0,0] = df[signal_fac_name].rank(pct=True).corr(df[next_ntick_return].rank(pct=True))
    return ic


def cal_monthly_ic(df,month:int,signal_fac_name:str,next_ntick_return:str,method='values'):
    """
    function:cal_monthly_ic
    parameters: df,month,signal_fac_name,next_ntick_return,method('values'--ic;'rank'--rank_ic)
    output:monthly_ic_df
    """ 
    ic = pd.DataFrame(np.zeros((1,1)),columns=['ic_monthly'],index = [month])
    ic.iloc[0,0] = df[signal_fac_name].corr(df[next_ntick_return])
    if method == 'values':
        ic.iloc[0,0] = df[signal_fac_name].corr(df[next_ntick_return])
    elif method == 'rank':
        ic.iloc[0,0] = df[signal_fac_name].rank(pct=True).corr(df[next_ntick_return].rank(pct=True))
    return ic

def multifactor_monthly_score(df_monthly,month,multi_fac_list,next_ntick_return,costline,method='OLS',index='hs300',future='if',constract = 'major'):
    """
    function: multifactor_regression
    freq_month = 20*freq_day(10)
    """
    reg_columns = copy.deepcopy(multi_fac_list)
    reg_columns.append(next_ntick_return)
    reg_columns.append('trade_date')
    reg_columns.append(f'{future}_{constract}_midPrc')
    reg_columns.append(f'{future}_{constract}_ask_price_1')
    reg_columns.append(f'{future}_{constract}_bid_price_1')
    
    df_monthly_new = df_monthly[reg_columns]   
    df_monthly_new = df_monthly_new.dropna().replace([np.inf,-np.inf],0)
    df_monthly_new['trade_time'] = df_monthly_new.index
    df_monthly_new['trade_datetime'] = df_monthly_new['trade_date']*1e6+df_monthly_new['trade_time']    
    df_monthly_new.set_index(['trade_datetime'],inplace=True)
    df_monthly_new = df_monthly_new.reset_index(drop=True)
    y = df_monthly_new[next_ntick_return]
    x = df_monthly_new[multi_fac_list]
    x = sm.add_constant(x)
    if method == "OLS" :
        LinearReg = sm.OLS(y,x).fit() 
    if method == "WLS" :        
        LinearReg = sm.WLS(y,x,weights=np.abs(y)+1e-7).fit() 
 
    y_pred = pd.DataFrame(LinearReg.predict(x),columns = ['score'])
    
    y_pred = pd.concat([y,y_pred,df_monthly_new[['trade_date','trade_time']]],axis=1)
    y_pred.columns=['ret','score','trade_date' ,'trade_time'] 
    y_pred= y_pred.sort_values(by=['score'],ascending=False)
       
    y_pred['rank'] = range(len(y_pred))
    y_pred['gp_1000'] = y_pred['rank'].apply(lambda x:int(x/len(y_pred)*1000))

    y_pred['open_positive_signal'] = 0
    y_pred['open_positive_signal'][y_pred['gp_1000'] == y_pred['gp_1000'].unique().min()] = 1
    y_pred['open_negative_signal'] = 0    
    y_pred['open_negative_signal'][y_pred['gp_1000'] == y_pred['gp_1000'].unique().max()] = 1
    
    y_pred['open_positive_signal_diff'] = (y_pred['open_positive_signal'] - y_pred['open_positive_signal'].shift(1)).replace(np.nan,0)
    y_pred['open_negative_signal_diff'] = (y_pred['open_negative_signal'] - y_pred['open_negative_signal'].shift(1)).replace(np.nan,0)

    y_pred['open_positive_signal_true'] = 0
    y_pred['open_positive_signal_true'][(y_pred['open_positive_signal'] == 1)&(y_pred['open_positive_signal_diff'] == 1)] = 1
    y_pred['open_negative_signal_true'] = 0
    y_pred['open_negative_signal_true'][(y_pred['open_negative_signal'] == 1)&(y_pred['open_negative_signal'] == 1)] = 1
   
    y_pred = y_pred.sort_values(by = ['trade_date','trade_time'],ascending = [True,True])
    y_pred = pd.concat([y_pred,df_monthly_new[[f'{future}_{constract}_midPrc',f'{future}_{constract}_ask_price_1',f'{future}_{constract}_bid_price_1']]],axis=1)
    
    y_pred['score_true'] = y_pred['score'].apply(lambda x: (x - costline) if x>=0 else (x + costline))

    return y_pred


def multifactor_monthly_test_final(df_monthly,month,multi_fac_list,next_return_dict,return_period,firtick = False,method='OLS',index='hs300'):
    """
    function: multifactor_regression
    freq_month = 20*freq_day(10)
    """
    import matplotlib.pyplot as plt
    df = copy.deepcopy(df_monthly)
    if firtick == False:
        df[next_return_dict[return_period]] = df[next_return_dict[return_period]].shift(-1)
    reg_columns = copy.deepcopy(multi_fac_list)
    reg_columns.append(next_return_dict[return_period])
    df = df[reg_columns]   
    df = df.dropna().replace([np.inf,-np.inf],0)
    res = pd.DataFrame(np.zeros((1,len(multi_fac_list)*2+1)))
    columns = copy.deepcopy(multi_fac_list)
    for fac in multi_fac_list:
        columns.append('tvalues_'+fac) 
    columns.append('Rsquared')

    res.columns = columns  
    y = df[next_return_dict[return_period]]
    x = df[multi_fac_list]
    x = sm.add_constant(x)
    if method == "OLS" :
        LinearReg = sm.OLS(y,x).fit() 
    if method == "WLS" :        
        LinearReg = sm.WLS(y,x,weights=np.abs(y)+1e-7).fit() 
 
#    LinearReg = sm.WLS(y,x,weights=np.abs(y)+1e-8).fit() 
    y_pred = LinearReg.predict(x)
    df_final = pd.concat([y,y_pred],axis=1)
    df_final.columns=[next_return_dict[return_period],next_return_dict[return_period]+'_pred']
    df_final= df_final.sort_values(by=[next_return_dict[return_period]+'_pred'],ascending=False)
    
    df_final["rank"] = range(len(df_final))
#    df_final["gp"] = df_final["rank"].apply(lambda x:int(x/len(df_final)*int(len(df_final)/(freq_day*20))))
    df_final["gp_500"] = df_final["rank"].apply(lambda x:int(x/len(df_final)*500))
    df_final["gp_1000"] = df_final["rank"].apply(lambda x:int(x/len(df_final)*1000))

    g_500 = df_final.groupby("gp_500")
    g_1000 = df_final.groupby("gp_1000")
        
    yy_500 = g_500[next_return_dict[return_period]].mean()
    xx_500 = g_500[next_return_dict[return_period]+'_pred'].mean()
    yy_1000 = g_1000[next_return_dict[return_period]].mean()
    xx_1000 = g_1000[next_return_dict[return_period]+'_pred'].mean()    

    plt.figure(figsize=(16, 9))
#    fig = plt.bar(xx,yy,width=1.0)
    fig1 = plt.plot(xx_500,yy_500,'ro',markersize=3) 
    plt.title("500groups_{}_{}".format(return_period,month))
    plt.savefig("/home/ybxu/future/{}_multi_fac_effective/500groups_{}_{}".format(index,return_period,month))
    plt.close()
    
    plt.figure(figsize=(16, 9))
    fig2 = plt.plot(xx_1000,yy_1000,'ro',markersize=3) 
    plt.title("1000groups_{}_{}".format(return_period,month))
    plt.savefig("/home/ybxu/future/{}_multi_fac_effective/1000groups_{}_{}".format(index,return_period,month))
    plt.close()    
   
    yy_500 = pd.DataFrame(yy_500)
    yy_500.columns = [str(month)]
    return yy_500

def cal_monthly_quantile_raw_return_decay(df,signal_fac_name:str,tvalues:int,K=10,N=10,retname="if_major_1tick_pnl"): 
    """
    function: signalfactor_regression with next n period return(n=1,2,3..,60)
              signalfactor is continue type
    parameters:df,month,signal_fac_name,N,method
    output:res['params','tvalues','rsquared']
    """
    df = df.reset_index(drop=True)
    if tvalues>0:      
        raw_return_decay = pd.DataFrame(np.zeros((N,1)),columns = ['maxvalues'])
        for decay in range(0,N):
            next_return = df[retname].shift(-(decay+1))
            rank_signal_factor = df[signal_fac_name].dropna().rank(pct=True)
            mask_maxvalues = ((rank_signal_factor>(1-1/K)) & (rank_signal_factor<1)) * 1        
            group_maxvalues = (mask_maxvalues.replace(0, np.nan) * next_return).mean()
            raw_return_decay.loc[decay,'maxvalues'] = group_maxvalues
    if tvalues<0:
        raw_return_decay = pd.DataFrame(np.zeros((N,1)),columns = ['minvalues'])
        for decay in range(0,N):
            next_return = df[retname].shift(-(decay+1))
            rank_signal_factor = df[signal_fac_name].dropna().rank(pct=True)
            mask_minvalues = ((rank_signal_factor<1/K) & (rank_signal_factor>0)) * 1            
            group_minvalues = (mask_minvalues.replace(0, np.nan) * next_return).mean()
            raw_return_decay.loc[decay,'minvalues'] = group_minvalues     
    raw_return_decay = raw_return_decay * 1e4
    return raw_return_decay


def cal_monthly_quantile_raw_return_decay_int(df,signal_fac_name:str,tvalues:int,N=10,retname="if_major_1tick_pnl"): 
    """
    function: signalfactor_regression with next n period return(n=1,2,3..,60)
              signalfactor is 0-1 type
    parameters:df,month,signal_fac_name,N,method
    output:res['params','tvalues','rsquared']
    """
    if tvalues>0:      
        raw_return_decay = pd.DataFrame(np.zeros((N,1)),columns = ['maxvalues'])
        for decay in range(0,N):
            next_return = df[retname].shift(-(decay+1))
            rank_signal_factor = df[signal_fac_name]
            mask_maxvalues = (rank_signal_factor == 1) * 1        
            group_maxvalues = (mask_maxvalues.replace(0, np.nan) * next_return).mean()
            raw_return_decay.loc[decay,'maxvalues'] = group_maxvalues
    if tvalues<0:
        raw_return_decay = pd.DataFrame(np.zeros((N,1)),columns = ['minvalues'])
        for decay in range(0,N):
            next_return = df[retname].shift(-(decay+1))
            rank_signal_factor = df[signal_fac_name]
            mask_minvalues = (rank_signal_factor == 1) * 1   
            group_minvalues = (mask_minvalues.replace(0, np.nan) * next_return).mean()
            raw_return_decay.loc[decay,'minvalues'] = group_minvalues     
    raw_return_decay = raw_return_decay * 1e4
    return raw_return_decay

