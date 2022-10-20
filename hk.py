import json
import urllib
from urllib import error
import urllib.request
from bs4 import BeautifulSoup
import bs4 as bs
import requests
import numpy as np
import sys
import time
import csv,os,os.path
import pandas as pd
from datetime import datetime,timedelta
import threading

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

		return('牛 '+str(day_n))

	if df['val'].iloc[-1]<0:
		day_n = 0
		for day in reversed(range(len(df))):
			if df['val'].iloc[day]<0:
				day_n+=1
			else:
				break
			
		return('熊 '+str(day_n))

def ema_x(df, n):
    EMA = pd.Series(df['close'].ewm(span=n, min_periods=n).mean(), name='ema_' + str(n))
    df = df.join(EMA)
    return df['ema_' + str(n)].iloc[-1]

def time_cmp(first_time, second_time):
    return int(time.strftime("%H%M%S", first_time)) - int(time.strftime("%H%M%S", second_time))

def sma_x(list,n):
	MA=round(sum(list[-n:])/n,2)
	return MA

def run_hk():

	today_date=datetime.today().strftime('%Y-%m-%d')

	# sauce = urllib.request.urlopen('http://hq.sinajs.cn/list=hk00700').read()
	# soup=BeautifulSoup(sauce,'html.parser')
	# soup=soup.decode()
	# last_p=float(soup.split(',')[6])
	# print('')
	# print(last_p,'\n')
	
	px_url='http://push2.eastmoney.com/api/qt/ulist.np/get?fields=f1,f2,f3,f4,f5,f6,f8,f12,f14,f13,f19,f29&secids=116.00700'
	sauce = urllib.request.urlopen(px_url).read()
	s1=json.loads(sauce)
	print(float(s1['data']['diff'][0]['f2'])/1000,'\n')
	last_p=float(s1['data']['diff'][0]['f2'])/1000
	
	BJ_time=soup.split(',')[-2].replace('/','-')
####
	
	FN_check='hk_single/HK.00700.csv'

	with open(FN_check,'r') as csvfile:
		reader = csv.reader(csvfile)
		rows = [row for row in reader]
	csvfile.close()

	while len(rows[-1])==0:
		rows.pop(-1)

	print(BJ_time,last_p,float(rows[-1][4]))
	if last_p==float(rows[-1][4]) and BJ_time==rows[-1][0]:
		print('Tencent same close price , Return')
		return()

	send_s='\nHK last traded on '+BJ_time
	f=open(r'run.csv','a+');f.write(send_s);f.close()

	left='http://47.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403567356536699773_1618882274635&pn='
	right='&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f6&fs=m:116+t:3,m:116+t:4,m:116+t:1,m:116+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_='

	f=open(r'hk.csv','w');f.write('');f.close()
	k=0;lst=[]
	today_date=datetime.today().strftime('%Y-%m-%d')
	emp2={'symbol':[],'name':[],'close':[],'o/i':[]};
	emp={'symbol':[],'name':[],'rate':[],'open':[],'high':[],'low':[],'close':[],'vol':[],'pe':[],'changehand':[]};
	hlc=pd.DataFrame(emp)
	hlc2=pd.DataFrame(emp)
	while k<=20:
		k=k+1	
		symbol=[];name=[];openp=[];high=[];low=[];close=[];vol=[];mv=[];turnover=[];rate=[];changed=[];ped=[]		
		url=left+str(k)+right			# sauce = urllib.request.urlopen(url).read()
		try:
			sauce = urllib.request.urlopen(url).read()
		except error.URLError as e:
			print(e.reason)
			break		
		soup=BeautifulSoup(sauce,'html.parser')
		soup=soup.decode()

		soup_f2=soup.split('f2":')[1:]
		for j in soup_f2:
			j2=j.split(',')[0]
			close.append(float(j2)) if j2!='"-"' else close.append(0)

		soup_rate=soup.split('f3":')[1:]
		for j in soup_rate:
			j2=j.split(',')[0]
			rate.append(j2) if j2!='"-"' else rate.append(0)
		
		soup_left=soup.split('f6":')[1:]

		for j in soup_left:	
			j2=j.split(',')[0]
			if j2!='"-"':
				turnover.append(float(j2))
			else:
				turnover.append(0)
				
			j2=j.split('f8":')[1]		
			changed.append(float(j2.split(',')[0])) if j2.split(',')[0]!='"-"' else changed.append(0)
			j2=j.split('f9":')[1]		
			ped.append(float(j2.split(',')[0])) if j2.split(',')[0]!='"-"' else ped.append(0)	

		soup=soup.split('f12":"')[1:]
		for j in soup:
			symbol.append(j.split('"')[0])
			j2=j.split('f14":"')[1]
			name.append(j2.split('"')[0])

			j2=j.split('f17":')[1]		
			openp.append(float(j2.split(',')[0])) if j2.split(',')[0]!='"-"' else openp.append(0)
			j2=j.split('f15":')[1]		
			high.append(float(j2.split(',')[0])) if j2.split(',')[0]!='"-"' else high.append(0)
			j2=j.split('f16":')[1]		
			low.append(float(j2.split(',')[0])) if j2.split(',')[0]!='"-"' else low.append(0)

			j2=j.split('f20":')[1]
			if j2.split(',')[0]!='"-"':
				mv.append(float(j2.split(',')[0]))		
			else:
				mv.append(0)
						
		for i in range(len(close)):
			if (close[i]>3):
				hlc2=hlc2.append({'symbol':'HK.'+symbol[i],'name':name[i],'close':close[i],'vol':turnover[i],'rate':rate[i],'pe':ped[i],'changehand':changed[i]},ignore_index=True)

				FN_insert='hk_single/HK.'+symbol[i]+'.csv'

				if len(symbol[i])==5:
				# if os.path.isfile(FN_insert) and len(symbol[i])==5:			
					csvFile_insert = open(FN_insert,'a+', newline='')
					write_insert = csv.writer(csvFile_insert)
					write3=[]
					write3.append(today_date)
					write3.append(openp[i])
					write3.append(high[i])
					write3.append(low[i])
					write3.append(close[i])
					write3.append(turnover[i])
					write_insert.writerow(write3);		#print(symbol[i])
					
				# elif len(symbol[i])==5:
					# FN_insert='hk_wait/HK.'+symbol[i]+'.csv'
					# csvFile_insert = open(FN_insert,'a+', newline='')
					# write_insert = csv.writer(csvFile_insert)
					# write3=[]
					# write3.append(today_date)
					# write3.append(openp[i])
					# write3.append(high[i])
					# write3.append(low[i])
					# write3.append(close[i])
					# write3.append(turnover[i])
					# write_insert.writerow(write3);		print('wait\t',symbol[i])
					
		print('k=',k)
		time.sleep(3)

# loop end 

	hlc2.sort_values(by='vol',ascending=False,inplace=True)
	hlc2=hlc2.sort_values(by='vol',ascending=False)
	hlc2['vol'] =hlc2.apply(lambda x: int(x[7] / 10000), axis=1)
	
	csvFile = open('report/'+BJ_time+'.csv','w', newline='')
	write_ok = csv.writer(csvFile)
	str_t=['代码','名称','成交额(万)','市盈率','换手率','收盘价','涨幅 %','100日涨幅 %',' SMA CROSS','趋势持续天数']
	write_ok.writerow(str_t)
	csvFile.close()

	csvFile = open('report/'+BJ_time+'.csv','a+', newline='')
	write_ok = csv.writer(csvFile)
	
	k=0
	for i in range(len(hlc2)):
		FN=hlc2['symbol'].iloc[i]+'.csv'		
		if os.path.isfile('hk_single/'+FN):
			FN_insert='hk_single/'+FN
			with open(FN_insert,'r') as csvfile:
				reader = csv.reader(csvfile)
				rows = [row for row in reader]
			csvfile.close()

			close_100=[]
			if len(rows)<100:
				print(FN,' : len < 100 , continue \n')
				str_t='\n'+str(datetime.now())[11:19]+FN+' len < 100  '
				f=open(r'log2.csv','a+');f.write(str_t);f.close()

				row_un=[]
				row_un.append(hlc2['symbol'].iloc[i])
				row_un.append(hlc2['name'].iloc[i])
				row_un.append(hlc2['vol'].iloc[i])
				row_un.append(hlc2['pe'].iloc[i])	
				row_un.append(hlc2['changehand'].iloc[i])	
				row_un.append(hlc2['close'].iloc[i])
				row_un.append(hlc2['rate'].iloc[i])
				row_un.append('')			
				row_un.append('')
				row_un.append('')
				write_ok.writerow(row_un)
				continue

			for i_c in range(100):
				close_100.append(float(rows[-i_c-1][4]))
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
			row_append=[]
			row_append.append(hlc2['symbol'].iloc[i])
			row_append.append(hlc2['name'].iloc[i])
			row_append.append(hlc2['vol'].iloc[i])
			row_append.append(hlc2['pe'].iloc[i])	
			row_append.append(hlc2['changehand'].iloc[i])	
			row_append.append(hlc2['close'].iloc[i])
			row_append.append(hlc2['rate'].iloc[i])
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
			if k<=50:
				rows=rows[-50:]
				df_emp={'symbol':[],'datetime':[],'open':[],'high':[],'low':[],'close':[]}
				df=pd.DataFrame(df_emp)
				for id in range(len(rows)):
					df=df.append({'datetime':rows[id][0],'open':float(rows[id][1]),'high':float(rows[id][2]),'low':float(rows[id][3]),'close':float(rows[id][4]),'volume':float(rows[id][5])},ignore_index=True)

				print('squeeze\t',k,hlc2['symbol'].iloc[i])
				trend+=szm_bb(df)

			row_append.append(trend)
			write_ok.writerow(row_append)
			k+=1
			if k>=1000:break
		else:
			continue
	csvFile.close()

	print('\n Write csv , Done \n')

	str_now=str(datetime.now())
	csvFile = open('run.csv','w', newline='')
	write_ok = csv.writer(csvFile)
	str_t=[str_now,' open line 330 ']
	write_ok.writerow(str_t)
	csvFile.close()

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
				f=open(r'log.csv','w');f.write('');f.close()
			time.sleep(30)	

if __name__ == '__main__':
	t = threading.Thread(target=t2)
	t.start()
	t.join()