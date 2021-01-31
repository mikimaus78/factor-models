# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 15:12:08 2020

@author: 64858
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
sys.path.append("/home/ybxu/scripts/")
from utils.utils_tick import get_trading_daysMonths,get_index_weight,signalfactor_monthly_test,\
                             signalfactor_monthly_parameter_decay_test,multifactor_monthly_test,\
                             multifactor_monthly_test_final                           
from config.fac_list_tick import ngroups,period,next_ntick_return,signal_fac_list,multi_fac_list,\
                                 return_period_list,next_return_dict,next_return_within_1tick_dict
from calfunc.cal_func import subsets
from load_dailydf.loaddf_calfac import load_daily_data
import warnings
warnings.filterwarnings('ignore')


class signalfac_test:
    def __init__(self,trade_month,index,constract,future,trading_day_list,trading_month_list,retname):
        self.index = index   
        self.constract = constract
        self.trade_month = trade_month
        self.future = future
        self.signal_fac_list = signal_fac_list
        self.trading_month_list = trading_month_list
        self.trading_day_list = trading_day_list
        self.next_ntick_return = next_ntick_return
        self.ngroups = ngroups
        self.retname = retname
    def signalfac_Tvaluesdecay_pairplot(self):
        buff_res = {}
        for fac in signal_fac_list:
            buff_res[fac] = pd.DataFrame()
        df_monthly = pd.DataFrame()
        trading_day_list_permonth  = [x for x in trading_day_list  if x//100 == trade_month]
        for day in trading_day_list_permonth:
            daily_df = load_daily_data(day,index,constract,future) 
            if len(df_monthly) == 0:
                df_monthly = daily_df
            else:
                df_monthly = df_monthly.append(daily_df)
            print(day)  
        #signal parameter decay test
        for fac in signal_fac_list:  
            buff_res[fac] = signalfactor_monthly_parameter_decay_test(df_monthly,trade_month,fac,retname,method="OLS")
            if len(res[fac]) == 0:
                res[fac] =  buff_res[fac]
            else: 
                res[fac] = pd.concat([res[fac],buff_res[fac]],axis=1)
            res[fac].to_excel("/home/ybxu/future/{}_signal_fac_effective/{}_ParameterDecayTvalues.xlsx".format(index,fac))   
        #signal factor pairplot
            df_monthly_new = pd.DataFrame([list(df_monthly[fac]), list(df_monthly[next_ntick_return])]).T
            df_monthly_new.columns = ["fac", "nextret"] 
            df_monthly_new = df_monthly_new.dropna()  
            df_monthly_new = df_monthly_new.sort_values(by="fac").reset_index(drop=True)
            
            df_monthly_new["rank"] = range(len(df_monthly_new))
            df_monthly_new["gp"] = df_monthly_new["rank"].apply(lambda x:int(x/len(df_monthly_new)*ngroups))
                
            g = df_monthly_new.groupby("gp")["fac","nextret"].mean()
            plt.figure(figsize=(16, 9))
            plt.title("{} signal distribution of {} ,{} groups".format(fac,next_ntick_return,str(ngroups)))
            plt.plot(g.fac,g.nextret,'ro',markersize=3)  
            plt.savefig("/home/ybxu/future/{}_signal_fac_effective/{}_{}_{}groups_{}_pairplot".format(index,fac,next_ntick_return,str(ngroups),str(trade_month)))
            plt.close()
            print(fac)

class multifac_test:
    def __init__(self,trade_month,index):
        self.index = index        
        self.trade_month = trade_month
        self.multi_fac_list = multi_fac_list
        self.trading_month_list = trading_month_list
        self.trading_day_list = trading_day_list
        self.next_ntick_return = next_ntick_return
        self.next_return_dict = next_return_dict
        self.next_return_within_1tick_dict = next_return_within_1tick_dict
        self.return_period_list = return_period_list
        self.ngroups = ngroups
        self.period = period        
    def multifac_regfilter(self):
    ##################fac filter 
        df_monthly = pd.DataFrame()
        trading_day_list_permonth  = [x for x in trading_day_list  if x//100 == trade_month]
        for day in trading_day_list_permonth:
            buff_daily = load_daily_data(day,index,constract,future)  
            if len(df_monthly) == 0:
                df_monthly = buff_daily
            else:
                df_monthly = df_monthly.append(buff_daily)
            print(day)  
        sub_mutifaclist = subsets(multi_fac_list)
        sub_mutifaclist = sorted(sub_mutifaclist,key = lambda i:len(i),reverse=False)
        df_reg = pd.DataFrame()
        for i in range(0,len(sub_mutifaclist)):
            buff = multifactor_monthly_test(df_monthly,trade_month,sub_mutifaclist[i],next_ntick_return,method="OLS")
            if len(df_reg) == 0:
                df_reg = buff
            else:
                df_reg = pd.concat([df_reg,buff]).reset_index(drop=True)
            print(sub_mutifaclist[i])       
        df_reg.to_excel("/home/ybxu/future/{}_signal_fac_effective/muti_reg_filter_{}tick_{}.xlsx".format(index,str(period),trade_month))
    def multifac_pairplot(self):
    ##################100 groups pairplot   
        df_final={}
        df_buff={}
        for return_period in return_period_list:
            df_final[return_period] = pd.DataFrame()
            df_buff[return_period] = pd.DataFrame()
        df_monthly = pd.DataFrame()
        trading_day_list_permonth  = [x for x in trading_day_list  if x//100 == trade_month]
        for day in trading_day_list_permonth:
            buff_daily = load_daily_data(day,index,constract,future)  
            if len(df_monthly) == 0:
                df_monthly = buff_daily
            else:
                df_monthly = df_monthly.append(buff_daily)
            print(day)    
            
        for return_period in return_period_list:
            df_buff[return_period] = multifactor_monthly_test_final(df_monthly,trade_month,multi_fac_list,next_return_dict,return_period,firtick = False,method="OLS",index='hs300')   
            if len(df_final[return_period]) == 0:
               df_final[return_period] =  df_buff[return_period]
            else:
               df_final[return_period] = pd.concat([df_final[return_period],df_buff[return_period]],axis=1)
            print(return_period)
        
        for return_period in return_period_list:
            df_final[return_period].to_excel("/home/ybxu/future/{}_multi_fac_effective/signal_performance_{}_500groups.xlsx".format(index,return_period))

  
if __name__ == '__main__':
    start_day = 20180101#trans start from 20171101(20171206 no sz)
    end_day = 20200331
    index = 'sz50'   
    constract = 'major'
    future = 'IH'
    retname = f'{future}_{constract}_1tick_pnl'
    trading_day_list,trading_month_list = get_trading_daysMonths(start_day,end_day)
    res = {}
    for fac in signal_fac_list:
        res[fac] = pd.DataFrame()
    for trade_month in trading_month_list:          
        action_signalfac_test= signalfac_test(trade_month,index,constract,future,trading_day_list,trading_month_list,retname)
        action_signalfac_test.signalfac_Tvaluesdecay_pairplot()
#        action_multifac_test= multifac_test(trade_month,index)
#        action_multifac_test.multifac_regfilter()
#        action_multifac_test.multifac_pairplot()
        
        
