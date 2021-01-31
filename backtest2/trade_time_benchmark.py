#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 12:27:02 2020

@author: ybxu
"""
#stock&future
import datetime
import pandas as pd
time_list = [(datetime.datetime(2018, 1, 1, 9, 30) + datetime.timedelta(seconds=i * 0.5))for i in range(1,45000)]
time_benchmark = pd.DataFrame(time_list,columns=['ftime'])
time_benchmark["ftime"] = time_benchmark["ftime"].apply(lambda x: x.hour*10000+x.minute*100+x.second+x.microsecond/1e6)
time_benchmark = time_benchmark[((time_benchmark<=145700)&(time_benchmark>130000))|(time_benchmark<=113000)&(time_benchmark>93000)].dropna()
time_benchmark = time_benchmark.reset_index(drop=True)
time_benchmark.to_pickle('/home/ybxu/data/time_benchmark.pkl')


#commodity
import datetime
import pandas as pd
time_list = [(datetime.datetime(2018, 1, 1, 9, 00) + datetime.timedelta(seconds=i * 0.5))for i in range(1,50000)]
time_benchmark = pd.DataFrame(time_list,columns=['ftime'])
time_benchmark["ftime"] = time_benchmark["ftime"].apply(lambda x: x.hour*10000+x.minute*100+x.second+x.microsecond/1e6)
time_benchmark = time_benchmark[((time_benchmark<=150000)&(time_benchmark>133000))|(time_benchmark<=101500)&(time_benchmark>90000)|(time_benchmark<=113000)&(time_benchmark>103000)].dropna()
time_benchmark = time_benchmark.reset_index(drop=True)
time_benchmark.to_pickle('/home/ybxu/data/commodity_time_benchmark.pkl')
