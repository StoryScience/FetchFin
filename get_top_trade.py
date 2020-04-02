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
    url = "http://quotes.money.163.com/service/chddata.html?code=0000001&start=20060101&fields=TCLOSE"
    s = requests.get(url).content.decode("gb2312")
    df = pd.read_csv(io.StringIO(s))
    date_set = set(df.iloc[:, 0])
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


def get_top_trade():
    def get_top_trade_detail(symbol, type, date):
        res = None
        for i in range(len(symbol)):
            url = "http://quotes.money.163.com/hs/marketdata/mrlhbSub.php?clear=1202&symbol={}&type={}&date={}".format(
                symbol[i], type[i], date)
            df = pd.read_html(url, encoding='utf-8')[1]
            df.drop(df.columns[len(df.columns) - 1], axis=1, inplace=True)
            length = int(len(df)//2)
            label1 = [df.iloc[0, 0]] * length
            label2 = [df.iloc[length, 0]] * length
            df["CODE"] = symbol[i]
            df["TDATE"] = date
            df["LABEL"] = label1 + label2
            df = df.drop([0, length])
            if res is None:
                res = df
            else:
                res = res.append(df)
        return res

    req_header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/13.11082',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        # 'Accept-Encoding':'gzip, deflate, compress, sdch, *',
        'Connection': 'keep-active',
    }
    folder_name = "top_trade"
    subfolder_name = "top_trade_details"

    creat_folder(folder_name, subfolder_name)

    date_list = get_date(folder_name)
    for date in date_list:
        try:
            print(date)
            url = "http://quotes.money.163.com/hs/marketdata/service/lhb.php?host=/hs/marketdata/service/lhb.php&page=0&query=start:{};end:{}&fields=NO,SYMBOL,SNAME,TDATE,TCLOSE,PCHG,SMEBTSTOCK1,SYMBOL,VOTURNOVER,COMPAREA,VATURNOVER,SYMBOL&sort=TDATE&order=desc&count=5000&type=query&".format(
                date, date)
            response = requests.get(url, headers=req_header)
            content = response.content
            js = json.loads(content)
            if js["pagecount"] == 0:
                continue
            print(js)
            df = pd.DataFrame(js["list"])
            df["SYMBOL"] = df["SYMBOL"].astype('str')
            df["CODE"] = df["CODE"].astype('str')
            df["SMEBTSTOCK11"] = df["SMEBTSTOCK11"].astype('str')

            print(df)

            df_detail = get_top_trade_detail(list(df["SYMBOL"]), list(df["SMEBTSTOCK11"]), date)

            file_name = os.path.join(folder_name, subfolder_name, date)
            df_detail.to_csv("{}.csv".format(file_name), index=False, encoding='utf_8_sig')
            print(df_detail)

            file_name = os.path.join(folder_name, date)
            df.to_csv("{}.csv".format(file_name), index=False, encoding='utf_8_sig')
        except Exception as err:
            print(err)
            pass


get_top_trade()
