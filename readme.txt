


=------------------------------

hk.csv 是便利所有港股，选取股价大于3的个股

cur.py 是读取hk.csv的股票代码 然后通过futu-api获取100个日线数据写入 hk_single 目录内，目录内的文件名是股票代码

等 hk_single 目录 900个个股数据写入完成后，下一天开始，
每天手动运行
python hk2.py -a

如果是自动运行
python hk2.py
程式会在收盘后16：10运行