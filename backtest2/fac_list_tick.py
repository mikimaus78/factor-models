#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 18:20:05 2020

@author: ybxu
"""


multi_fac_list =[
 'hs300_lc_20tick',
 'zz500_lc_20tick',
 'hs300_hc_20tick',
 'zz500_hc_20tick',
 'hs300_cal_pnl_15tick_mean',
 'zz500_cal_pnl_15tick_mean',
 'hs300_cal_rsi',
 'zz500_cal_rsi',
 'sz50_cal_rsi',
 'zz500_winloseratio',
 'sz50_winloseratio']

signal_fac_list =[
 'hs300_lc_20tick',
 'zz500_lc_20tick',
 'hs300_hc_20tick',
 'zz500_hc_20tick',
 'hs300_cal_pnl_15tick_mean',
 'zz500_cal_pnl_15tick_mean',
 'hs300_cal_rsi',
 'zz500_cal_rsi',
 'sz50_cal_rsi',
 'zz500_winloseratio',
 'sz50_winloseratio']

shorttime_multi_fac_list = ['IF_volume_ask1bid1_spread','IF_volume_bid1_diff','IF_volume_ask1_diff',
                  'IF_price_bid1_diff','IF_price_ask1_diff']

ngroups = 100
period = 20
next_ntick_return = f'next_return_2to{period}tick_total'  #'next_return_1to20tick_total'
return_period_list=[5,10,20,60,120]


next_return_within_1tick_dict={5:'next_return_1to5tick_total',
                  10:'next_return_1to10tick_total',
                  20:'next_return_1to20tick_total',
                  60:'next_return_1to60tick_total',
                  120:'next_return_1to120tick_total',
                  }

next_return_dict={5:'next_return_2to5tick_total',
                  10:'next_return_2to10tick_total',
                  20:'next_return_2to20tick_total',
                  60:'next_return_2to60tick_total',
                  120:'next_return_2to120tick_total',
                  }