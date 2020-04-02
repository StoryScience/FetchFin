import requests
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

    start_date = date(2018, 3, 9)
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


def get_dde():

    folder_name = "dde"

    creat_folder(folder_name, "")

    date_list = get_date(folder_name)

    for date in date_list:
        try:
            url = 'http://www.ddxzx.com/ygetnewallddxpm.php?orderby=0&lsdate=' + str(date) + '&d=sz&page=1'

            req_header = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/13.11082',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                # 'Accept-Encoding':'gzip, deflate, compress, sdch, *',
                'Connection': 'keep-active',
                'Referer': url
            }

            response = requests.get(url, headers=req_header)
            content = response.content

            if content == '{"errmsg":"nodata"}':
                print('Error, No data!')
            else:
                df = pd.read_json(content)
                my_amount = df.total[0]
                iterate = int(my_amount / 20)
                if my_amount / 20 != iterate:
                    iterate = iterate + 1

                total_data = None
                for page in range(iterate):
                    page = page + 1
                    print(page, "of", iterate)
                    url = 'http://www.ddxzx.com/ygetnewallddxpm.php?orderby=0&lsdate=' + str(date) + '&d=sz&page=' + str(
                        page)
                    req_header = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/13.11082',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        # 'Accept-Encoding':'gzip, deflate, compress, sdch, *',
                        'Connection': 'keep-active',
                        'Referer': url
                    }

                    response = requests.get(url, headers=req_header)
                    content = response.content
                    # content = content.replace("\r\n","")
                    df = pd.read_json(content)

                    my_date = str(date)
                    my_data = df.data

                    lenth = len(my_data)
                    temp = []
                    for i in range(lenth):
                        temp.append(my_data[i])
                    my_data = temp

                    my_data = pd.DataFrame(my_data, columns= \
                        ['Symbol', 'Price', 'PriceChange', 'TurnoverRate', 'VolumeRatio', 'DDX', \
                         'DDY', 'DDZ', '5DayDDE', '10DayDDE', 'Continuation', 'ContinualRise', 'Volume', 'BBD', \
                         'SellBuyRatio', 'BuyAmount', 'SellAmount', 'SuperTradeDifference', \
                         'BigTradeDifference', 'MediumTradeDifference', 'SmallTradeDifference', 'SuperTradeBuyRatio', \
                         'SuperTradeSellRatio', 'BigTradeBuyRatio', 'BigTradeSellRatio', 'SmallTradeBuyRatio', \
                         'SmallTradeSellRatio', '1DayTakeRate', '5DayTakeRate', '10DayTakeRate', '20DayTakeRate', \
                         '1DayActiveRate', '5DayActiveRate', '10DayActiveRate', 'Capital'])
                    my_data['Date'] = my_date
                    # my_data['Scalar'] = 100
                    my_data['Capital'] = my_data['Capital'].astype('float64')
                    my_data['TurnoverRate'] = my_data['TurnoverRate'].astype('float64')
                    my_data['SuperTradeBuyRatio'] = my_data['SuperTradeBuyRatio'].astype('float64')
                    my_data['SuperTradeSellRatio'] = my_data['SuperTradeSellRatio'].astype('float64')
                    my_data['BigTradeBuyRatio'] = my_data['BigTradeBuyRatio'].astype('float64')
                    my_data['BigTradeSellRatio'] = my_data['BigTradeSellRatio'].astype('float64')
                    my_data['SmallTradeBuyRatio'] = my_data['SmallTradeBuyRatio'].astype('float64')
                    my_data['SmallTradeSellRatio'] = my_data['SmallTradeSellRatio'].astype('float64')

                    my_data['TurnoverRate'] = my_data['TurnoverRate'] / 100
                    my_data['SuperTradeBuyRatio'] = my_data['SuperTradeBuyRatio'] / 100
                    my_data['SuperTradeSellRatio'] = my_data['SuperTradeSellRatio'] / 100
                    my_data['BigTradeBuyRatio'] = my_data['BigTradeBuyRatio'] / 100
                    my_data['BigTradeSellRatio'] = my_data['BigTradeSellRatio'] / 100
                    my_data['SmallTradeBuyRatio'] = my_data['SmallTradeBuyRatio'] / 100
                    my_data['SmallTradeSellRatio'] = my_data['SmallTradeSellRatio'] / 100

                    print(my_data)
                    if total_data is None:
                        total_data = my_data
                    else:
                        total_data = total_data.append(my_data)

                file_name = os.path.join(folder_name, date)
                total_data.to_csv("{}.csv".format(file_name), index=False, encoding='utf_8_sig')
        except Exception as err:
            print(err)
            pass


get_dde()
