import urllib.request
import requests
import numpy as np
import pandas as pd
import sys,time,csv,os,json,urllib
from datetime import datetime,timedelta
import os.path
from futu import *
hlc2=pd.DataFrame({})

filetype ='.csv'

df = pd.read_csv('futu.csv', dtype='str',engine='python', names=['code','name','sector'], encoding='utf-8')
print(len(df))
df.drop_duplicates(subset=None, keep='first', inplace=True)
print(df,len(df))
rows = list(df['code'])

hsitech_k=len(df)

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
ST=SubType.K_DAY;	k_n=250		
print('read.py,',hsitech_k)
while hsitech_k>0:

	symbol=rows[-hsitech_k]
	# print(len(symbol),symbol)
	hsitech_k-=1

	ret_sub, err_message = quote_ctx.subscribe(symbol, [ST], subscribe_push=False)
	if ret_sub==-1:
		print(err_message)
		continue
	elif ret_sub == RET_OK:
		time.sleep(0.5)
		ret, df = quote_ctx.get_cur_kline(symbol, k_n, ST)
		if len(df)>=100:
			FN='hk_single/'+symbol+'.csv'
			df.to_csv(FN, index=False, mode='w')
			
			
			hlc2 = hlc2.append(df.tail(1))
			
			print(symbol,'csv write done \t len ',len(hlc2))
			
		elif ret!=-1 and len(data3)<100:
			print('\n Less than 100 , continue \n')
			continue

		elif ret==-1:
			print(data3)
			print('Error , Exit')
			quote_ctx.close()
			sys.exit()
hlc2.to_csv('report/hlc2.csv', index=False, mode='w')
print('all stocks saved')
quote_ctx.close()
sys.exit()