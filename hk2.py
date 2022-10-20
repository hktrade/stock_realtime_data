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

	sauce = urllib.request.urlopen('http://hq.sinajs.cn/list=hk00700').read()
	soup=BeautifulSoup(sauce,'html.parser')
	soup=soup.decode()
	last_p=float(soup.split(',')[6])
	print('')
	print(last_p,'\n')	
	px_url='http://push2.eastmoney.com/api/qt/ulist.np/get?fields=f1,f2,f3,f4,f5,f6,f8,f12,f14,f13,f19,f29&secids=116.00700'
	sauce = urllib.request.urlopen(px_url).read()
	s1=json.loads(sauce)
	print(float(s1['data']['diff'][0]['f2'])/1000,'\n')
	last_p=float(s1['data']['diff'][0]['f2'])/1000
	
	BJ_time=soup.split(',')[-2].replace('/','-')
	
	try:
		FN_check='hk_single/HK.00700.csv'

		with open(FN_check,'r') as csvfile:
			reader = csv.reader(csvfile)
			rows = [row for row in reader]
		csvfile.close()

		while len(rows[-1])==0:
			rows.pop(-1)
			
		print(BJ_time,'js csv\t',last_p,float(rows[-1][4]))
		if last_p==float(rows[-1][4]):
			print('Tencent same close price , Return')
			return()
	except Exception as e:
		print(format(e))
			
	send_s='\nHK last traded on '+BJ_time
	f=open(r'run.csv','a+');f.write(send_s);f.close()

	#http://quote.eastmoney.com/center/gridlist.html#hk_stocks
	# http://33.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112407517752566195535_1593946093611&pn=1&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=
	# jQuery112407517752566195535_1593946093611

	# left='http://33.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112407517752566195535_1593946093611&pn='
	# right='&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_='

	#<date>,<open>,<high>,<low>,<close>,<vol>
	# 'http://47.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403567356536699773_1618882274635&pn=1&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f6&fs=m:116+t:3,m:116+t:4,m:116+t:1,m:116+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_=1618882274676'

	left='http://47.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403567356536699773_1618882274635&pn='
	right='&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f6&fs=m:116+t:3,m:116+t:4,m:116+t:1,m:116+t:2&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152&_='

	f=open(r'hk.csv','w');f.write('');f.close()
	k=0;lst=[]
	today_date=datetime.today().strftime('%Y-%m-%d')
	# emp={'symbol':[],'name':[],'per':[],'date':[],'time':[],'open':[],'high':[],'low':[],'close':[],'vol':[],'o/i':[]};
	emp={'symbol':[],'name':[],'close':[],'vol':[],'o/i':[]};
	emp2={'symbol':[],'name':[],'close':[],'o/i':[]};
	hlc=pd.DataFrame(emp)
	hlc2=pd.DataFrame(emp)
	while k<=20:
		k=k+1	
		symbol=[];name=[];openp=[];high=[];low=[];close=[];vol=[];mv=[];turnover=[];rate=[]
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
			if (close[i]>0):
				# hlc2=hlc2.append({'symbol':symbol[i],'name':name[i],'per':'D','date':'20200709','time':'0','open':openp[i],'high':high[i],'low':low[i],'close':close[i],'vol':turnover[i],'o/i':rate[i]},ignore_index=True)
				hlc2=hlc2.append({'symbol':symbol[i],'name':name[i],'close':close[i],'vol':turnover[i],'o/i':rate[i]},ignore_index=True)

				FN_insert='hk_single/HK.'+symbol[i]+'.csv'
				if os.path.isfile(FN_insert) and len(symbol[i])==5:			
					csvFile_insert = open(FN_insert,'a+', newline='')
					write_insert = csv.writer(csvFile_insert)
					write3=[]
					write3.append(today_date)
					write3.append(openp[i])
					write3.append(high[i])
					write3.append(low[i])
					write3.append(close[i])
					write3.append(turnover[i])
					write_insert.writerow(write3);		#print(FN_insert)
				elif len(symbol[i])==5:
					FN_insert='hk_wait/HK.'+symbol[i]+'.csv'
					csvFile_insert = open(FN_insert,'a+', newline='')
					write_insert = csv.writer(csvFile_insert)
					write3=[]
					write3.append(today_date)
					write3.append(openp[i])
					write3.append(high[i])
					write3.append(low[i])
					write3.append(close[i])
					write3.append(turnover[i])
					write_insert.writerow(write3);		#print(FN_insert)
					
		print('k=',k)
		time.sleep(3)
	
	print('\n Write csv , Done \n');#sys.exit()

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