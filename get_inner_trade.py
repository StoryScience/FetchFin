import requests
import json
import os
import io
import pandas as pd
from datetime import date, timedelta


def creat_folder(folder_name, subfolder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    if not os.path.exists(os.path.join(folder_name, subfolder_name)):
        os.makedirs(os.path.join(folder_name, subfolder_name))


def get_trade_date():
    url = "http://quotes.money.163.com/service/chddata.html?code=0000001&start=20090101&fields=TCLOSE"
    s = requests.get(url).content.decode("gb2312")
    df = pd.read_csv(io.StringIO(s))
    date_set = set(df.iloc[:, 0])
    date_set_prev = get_inner_trade_date()
    total_date_set = date_set.union(date_set_prev)

    return total_date_set


def get_inner_trade_date(start="2007-01-01", end="2009-02-01"):
    url = "http://quotes.money.163.com/hs/marketdata/service/nbjy.php?host=/hs/marketdata/service/nbjy.php&page=0&query=start:{};end:{};&fields=NO,SYMBOL,SNAME,CNAME,MHOLDER5,MHOLDER6,MHOLDER7,BIANDONGJINE,MHOLDER2,MHOLDER4,RELATION,DJZW,REPORTDATE&sort=REPORTDATE&order=desc&count=5000&type=query&".format(start, end)

    req_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/13.11082',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        # 'Accept-Encoding':'gzip, deflate, compress, sdch, *',
        'Connection': 'keep-active',
        # 'Referer': url
    }

    response = requests.get(url, headers=req_header)
    content = response.content
    js = json.loads(content)

    df = pd.DataFrame(js["list"])
    date_set = set(df["REPORTDATE"])

    return date_set

def get_local_date(folder_name):
    date_set = set()
    for _, _, files in os.walk(folder_name):
        for filename in files:
            file_name = os.path.splitext(filename)[0]
            date_set.add(file_name)
    return date_set


def get_date(folder_name):
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    start_date = date(2007, 1, 1)
    end_date = date.today()
    local_date_set = get_local_date(folder_name)
    trade_date_set = get_trade_date()

    date_list = []
    for single_date in daterange(start_date, end_date + timedelta(1)):
        date_time = single_date.strftime("%Y-%m-%d")
        # print(trade_date_set, local_date_set)
        if date_time not in local_date_set and date_time in trade_date_set:
            date_list.append(str(date_time))
    return date_list


def get_inner_trade():
    req_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/13.11082',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        # 'Accept-Encoding':'gzip, deflate, compress, sdch, *',
        'Connection': 'keep-active',
        # 'Referer': url
    }

    folder_name = "inner_trade"

    creat_folder(folder_name, "")

    date_list = get_date(folder_name)

    for date in date_list:

        try:

            url = "http://quotes.money.163.com/hs/marketdata/service/nbjy.php?host=/hs/marketdata/service/nbjy.php&page=0&query=start:{};end:{};&fields=NO,SYMBOL,SNAME,CNAME,MHOLDER5,MHOLDER6,MHOLDER7,BIANDONGJINE,MHOLDER2,MHOLDER4,RELATION,DJZW,REPORTDATE&sort=REPORTDATE&order=desc&count=5000&type=query&".format(date, date)

            response = requests.get(url, headers=req_header)
            content = response.content
            js = json.loads(content)
            if js["pagecount"] == 0:
                continue
            df = pd.DataFrame(js["list"])
            print(df)
            if len(df) > 0:
                print(df)
                df["SYMBOL"] = df["SYMBOL"].astype('str')
                df["CODE"] = df["CODE"].astype('str')

                print(df)
                file_name = os.path.join(folder_name, date)
                df.to_csv("{}.csv".format(file_name), index=False, encoding='utf_8_sig')
        except Exception as err:
            print(err)
            pass

get_inner_trade()
