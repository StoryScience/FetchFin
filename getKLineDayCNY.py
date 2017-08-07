#!/usr/bin/python
#-*-coding:utf-8-*-

import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import MySQLdb
import urllib2
import sys
import json
reload(sys)
sys.setdefaultencoding('utf8')


def database_operation():
    conn = MySQLdb.connect(db='bitcoin_database', host='localhost', user='root', passwd='root')
    cur = conn.cursor()
    cur.execute('SET SQL_SAFE_UPDATES = 0')
    cur.execute('CREATE SCHEMA IF NOT EXISTS bitcoin_database')
    # cur.execute('DROP TABLE IF EXISTS bitcoin_database.daily_data_cny')
    cur.execute('DROP TABLE IF EXISTS bitcoin_database.err_temp_daily_cny')
    cur.execute('CREATE TABLE IF NOT EXISTS bitcoin_database.err_temp_daily_cny (Symbol varchar(20))')
    cur.execute('''CREATE TABLE IF NOT EXISTS bitcoin_database.daily_data_cny 
        (ID INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
        Date date,
        Symbol VARCHAR(20),
        Open double,
        High double,
        Low double,
        Close double,
        Volume double)
        AUTO_INCREMENT = 1;
        ''')

    try:
        cur.execute('ALTER TABLE bitcoin_database.daily_data_cny ADD INDEX index_name (Symbol, Date)')
        #cur.execute('ALTER TABLE bitcoin_database.daily_data_cny DROP INDEX index_name')
    except:
        pass
    conn.commit()
    cur.close
    conn.close


def getStartTimeStamp(symbol):
    conn = MySQLdb.connect(db='bitcoin_database', host='localhost', user='root', passwd='root')
    cur = conn.cursor()
    cur.execute('SELECT MAX(Date) FROM bitcoin_database.daily_data_cny WHERE Symbol = \'%s\'' % symbol)
    max_date = cur.fetchone()

    if max_date[0] is not None:
        max_date = str(max_date[0])
        # 转换成时间数组
        timeArray = time.strptime(max_date, "%Y-%m-%d")
        # 转换成时间戳
        timeStamp = time.mktime(timeArray)
        timeStamp = int(timeStamp*1000)
        return timeStamp+1
    else:
        return 1199097600001

    cur.close
    conn.close


def get_k_data(sym):
    global err_signal
    try:
        data = pd.DataFrame()
        timeStamp = getStartTimeStamp(sym)
        url = 'https://www.okcoin.cn/api/v1/kline.do?symbol='+sym+'&type=1day&since=' + str(timeStamp)
        # url = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_day&param=sz300002,day,2006-01-01,2007-12-31,10000,qfq&r=0.15573139310503694'
        # req_header = {
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
        #             (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/13.11082',
        #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #     #'Accept-Encoding':'gzip, deflate, compress, sdch, *',
        #     'Connection': 'keep-active',
        #     'Referer': url
        # }
        # print url

        req = urllib2.Request(url)
        resp = urllib2.urlopen(req, timeout=3)
        content = resp.read()
        js = json.loads(content)

        cols = ['Date', 'Open',  'High', 'Low', 'Close', 'Volume']
        df = pd.DataFrame(js, columns=cols)
        # print df
        timeStamp = list(df['Date'].astype('int64'))
        # print timeStamp
        otherStyleTime = []
        for i in timeStamp:
            temp = time.localtime(i/1000)
            temp = time.strftime('%Y-%m-%d', temp)
            # temp = time.strftime('%Y-%m-%d %H:%M:%S', temp)
            otherStyleTime.append(temp)

        df['Date'] = otherStyleTime
        df['Symbol'] = sym
        data = data.append(df)
        print data

        data.to_sql('daily_data_cny', engine, index=0, if_exists='append')
        print data
        err_signal = 0

    except Exception, e:
        cur.execute('INSERT INTO bitcoin_database.err_temp_daily_cny (Symbol) VALUES(\"%s\")' % sym)
        conn.commit()
        err_signal = 1
        print e
        pass


def error_rectify():
    lenth = 1000
    while lenth > 0:
        cur.execute('SELECT DISTINCT Symbol FROM bitcoin_database.err_temp_daily_cny')
        symbol_dic = cur.fetchall()
        lenth = len(symbol_dic)

        for dic in symbol_dic:
            symbol = dic[0]
            try:
                get_k_data(symbol)

                if err_signal == 0:
                    cur.execute('DELETE FROM bitcoin_database.err_temp_daily_cny WHERE Symbol = \"%s\"' % symbol)
                    conn.commit()
            except Exception, e:
                print e
                pass


if __name__ == '__main__':

    global engine
    global conn
    global start_year
    global end_year
    engine = create_engine('mysql://root:root@127.0.0.1/bitcoin_database?charset=utf8')
    conn = MySQLdb.connect(db='bitcoin_database', host='localhost', user='root', passwd='root')
    cur = conn.cursor()
    cur.execute('SET SQL_SAFE_UPDATES = 0')
    conn.commit()

    database_operation()

    symbol_dic = ['btc_cny', 'ltc_cny', 'eth_cny']

    for sym in symbol_dic:
        get_k_data(sym)
    error_rectify()
    cur.execute('DROP TABLE IF EXISTS bitcoin_database.err_temp_daily_cny')
    conn.commit()
    cur.close
    conn.close
    print 'getKData OK!'
