import json,urllib,sys,time,requests,threading
from urllib import error
import urllib.request
from bs4 import BeautifulSoup
import bs4 as bs
import numpy as np
import csv,os,os.path
import pandas as pd
from datetime import datetime,timedelta

def szm_bb(df):
	# df = ema_x(df, 21)	
	df['20sma'] = df['close'].rolling(window=20).mean()
	df['stddev'] = df['close'].rolling(window=20).std()
	df['bbl'] = df['20sma'] - (2 * df['stddev'])
	df['bbh'] = df['20sma'] + (2 * df['stddev'])
	df['20ema'] =df['close'].ewm(span=20, min_periods=20).mean()
	# df['source'] = (df['close'].rolling(window=20).max() + df['close'].rolling(window=20).min() + df['close'].rolling(window=20).mean())/3
	df['source'] = (df['high'].rolling(window=20).max() + df['low'].rolling(window=20).min() + df['close'].rolling(window=20).mean())/3

	df['val'] = df['close'] - df['source']		
	if df['close'].iloc[-1]>df['bbh'].iloc[-1]:
		return('Breakout')
	if df['close'].iloc[-1]<df['bbl'].iloc[-1]:
		return('Breakdown')

	if df['val'].iloc[-1]>0:
		day_n = 0
		for day in reversed(range(len(df))):
			if df['val'].iloc[day]>0:
				day_n+=1
			else:
				break

		return('Bull '+str(day_n))

	if df['val'].iloc[-1]<0:
		day_n = 0
		for day in reversed(range(len(df))):
			if df['val'].iloc[day]<0:
				day_n+=1
			else:
				break
			
		return('Bear '+str(day_n))
def sma_x(list,n):
	MA=round(sum(list[-n:])/n,2)
	return MA
def ema_x(df, n):
    EMA = pd.Series(df['close'].ewm(span=n, min_periods=n).mean(), name='ema_' + str(n))
    df = df.join(EMA)
    return df['ema_' + str(n)].iloc[-1]

def run_hk():
	os.system(r'python read.py')
	print('Finished running read.py')
	today_date=datetime.today().strftime('%Y-%m-%d')

	try:
		FN_check='hk_single/HK.00700.csv'
		df = pd.read_csv(FN_check,engine='python',encoding='utf-8')
		print(df.tail(1))
		
	except Exception as e:
		print(format(e))

	dfah = pd.read_csv('futu.csv', dtype='str',engine='python', names=['code','name','sector'], encoding='utf-8')
	rows = list(dfah['code'])
	
	k=0;lst=[]
	today_date=datetime.today().strftime('%Y-%m-%d')

# code,time_key,open,close,high,low,volume,turnover,pe_ratio,turnover_rate

	hlc2 = pd.read_csv('report/hlc2.csv', engine='python')
	hlc2['time_key']=hlc2['time_key'].str.replace(' 00:00:00','')
	hlc2.sort_values(by='turnover',ascending=False,inplace=True)
	hlc2=hlc2.sort_values(by='turnover',ascending=False)
	hlc2['turnover'] =hlc2.apply(lambda x: int(x[7] / 10000), axis=1)
	# hlc2['pe_ratio']=hlc2['pe_ratio'].round(decimals=2)
	# hlc2['turnover_rate']=hlc2.apply(lambda x: int(x[9] * 100), axis=1)
	# hlc2['turnover_rate']=hlc2['turnover_rate'].round(decimals=2)
	hlc2 = pd.merge(hlc2, dfah, on='code', how='inner')
	print(hlc2)
	
	csvFile = open('report/today.csv','w', newline='')
	write_ok = csv.writer(csvFile)
	str_t=['代碼','名稱','板塊','成交額(万)','市盈率','換手率','收盤價','漲跌幅 %','100日漲跌幅 %',' SMA CROSS ','Trending']
	write_ok.writerow(str_t)
	csvFile.close()
	
# code,name,turnover,pe_ratio,turnover_rate,close,rate,rate100,sma,trend

	csvFile = open('report/today.csv','a+', newline='')
	write_ok = csv.writer(csvFile)
	
	k=0
	for i in range(len(hlc2)):
		FN=hlc2['code'].iloc[i]+'.csv'		
		if os.path.isfile('hk_single/'+FN):
			FN_insert='hk_single/'+FN
# code,name,sector,turnover,pe_ratio,turnover_rate,close,rate,rate100,ema,trend
			with open(FN_insert,'r') as csvfile:
				reader = csv.reader(csvfile)
				rows = [row for row in reader]
			csvfile.close()
			close_100=[]
			for c in range(100):
				close_100.append(float(rows[-c-1][3]))
			close_100.reverse()
			avg10=(sma_x(close_100,10));avg20=(sma_x(close_100,20))
			avg30=(sma_x(close_100,30));avg40=(sma_x(close_100,40));avg50=(sma_x(close_100,50));avg60=(sma_x(close_100,60))
			avg70=(sma_x(close_100,70));avg80=(sma_x(close_100,80));avg90=(sma_x(close_100,90));avg100=(sma_x(close_100,100))
			SAVE_X='0'
			if avg10>avg20:
				SAVE_X='10'
				if avg20>avg30:
					SAVE_X='20'
					if avg30>avg40:
						SAVE_X='30'
						if avg40>avg50:
							SAVE_X='40'
							if avg50>avg60:
								SAVE_X='50'
								if avg60>avg70:
									SAVE_X='60'
									if avg70>avg80:
										SAVE_X='70'
										if avg80>avg90:
											SAVE_X='80'
											if avg90>avg100:
												SAVE_X='90'
			elif avg10<avg20:
				SAVE_X='-10'
				if avg20<avg30:
					SAVE_X='-20'
					if avg30<avg40:
						SAVE_X='-30'
						if avg40<avg50:
							SAVE_X='-40'
							if avg50<avg60:
								SAVE_X='-50'
								if avg60<avg70:
									SAVE_X='-60'
									if avg70<avg80:
										SAVE_X='-70'
										if avg80<avg90:
											SAVE_X='-80'
											if avg90<avg100:
												SAVE_X='-90'
#''市盈率','换手率','收盘价','涨幅',' SMA']
# code,name,sector,turnover,pe_ratio,turnover_rate,close,rate,rate100,ema,trend
			
			row_append=[]
			row_append.append(hlc2['code'].iloc[i])
			row_append.append(hlc2['name'].iloc[i])
			row_append.append(hlc2['sector'].iloc[i])
			row_append.append(hlc2['turnover'].iloc[i])
			row_append.append(hlc2['pe_ratio'].iloc[i])	
			row_append.append(hlc2['turnover_rate'].iloc[i])	
			row_append.append(hlc2['close'].iloc[i])
			rate = round((close_100[-1]-close_100[-2])/close_100[-2]*100,2)
			# print(row_append)
			# print(hlc2['turnover_rate'].iloc[i])
			# print(close_100[-1],close_100[-2])
			# sys.exit()
			row_append.append(rate)
			row_append.append(round((close_100[-1]-close_100[0])/close_100[0]*100,2))			
			row_append.append(SAVE_X)			
			trend=''			
			cl_hl = (close_100[-1] - min(close_100)) / (max(close_100) - min(close_100))			
			if cl_hl > 0.66:
				trend = '高  '
			elif cl_hl > 0.33:
				trend = '中  '
			else:
				trend = '低  '
# 50 - 20 = 30
			if k<=100:
				rows=rows[-50:]

				print('squeeze\t',k,hlc2['code'].iloc[i])
				df = pd.read_csv(FN_insert,engine='python',encoding='utf-8')
				trend+=szm_bb(df)

			row_append.append(trend)
			write_ok.writerow(row_append)
			k+=1
			if k>=100:break
		else:
			continue
	csvFile.close()

	print('\n Write csv , Done \n')

def t2():
	while 1:
		wk=datetime.today().weekday()
		if wk<5:
			if (int(str(datetime.now())[11:13])==16) and (int(str(datetime.now())[14:16])==10 or int(str(datetime.now())[14:16])==11):
				str_now=str(datetime.now())
				FN_csv='run.csv'
				with open(FN_csv,'r') as csvfile:
					reader = csv.reader(csvfile)
					rows = [row for row in reader]
				csvfile.close
				today_date=datetime.today().strftime('%Y-%m-%d')
				if len(rows)>0 and (str(rows[0]).find('open')==-1 or str(rows[0]).find(today_date)==-1):
					run_hk()
					time.sleep(150)
				else:
					if (str(datetime.now())[15:16]=='0'):
						csvFile = open('run.csv','a+', newline='')
						write_ok = csv.writer(csvFile)
						str_t=[str_now,' at time but else']
						write_ok.writerow(str_t)
						csvFile.close()
			elif str(sys.argv).find('-a')!=-1:
				print(sys.argv)
				sys.argv[-1]=sys.argv[-1].replace('-a','')
				print(sys.argv)

				run_hk()
				time.sleep(150)
			else:
				if (str(datetime.now())[15:16]=='0'):
					str_now=str(datetime.now())
					csvFile = open('run.csv','a+', newline='')
					write_ok = csv.writer(csvFile)
					str_t=[str_now,' sleeping']
					write_ok.writerow(str_t)
					csvFile.close()
			time.sleep(30)
	
		
		elif str(sys.argv).find('-a')!=-1:
			print(sys.argv)
			sys.argv[-1]=sys.argv[-1].replace('-a','')
			print(sys.argv)
				
			run_hk()
			time.sleep(150)
		
		else:			
			str_now=str(datetime.now())
			if (str(datetime.now())[15:16]=='0'):
				csvFile = open('run.csv','a+', newline='')
				write_ok = csv.writer(csvFile)
				str_t=[str_now,' HK Weekend']
				write_ok.writerow(str_t)
				csvFile.close()
			time.sleep(30)	

if __name__ == '__main__':
	t = threading.Thread(target=t2)
	t.start()
	t.join()