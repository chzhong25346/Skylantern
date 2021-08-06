from datetime import datetime as dt
import requests, re
import pandas as pd
import numpy as np
import json
import time
import os
import tushare as ts
import logging
from bs4 import BeautifulSoup
logger = logging.getLogger('main.fetch')


def fetch_index(index_name):
    path = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(path, index_name+'.csv')
    try:
        if (index_name == 'csi300'):
            data = pd.read_csv(filename)
            data.columns = ['symbol', 'company']

            # data = data.drop(['lastsale', 'netchange', 'netchange', 'share_volume', 'Nasdaq100_points', 'Unnamed: 7'], axis=1)
            data.index.name = 'symbol'
            return data
    except Exception as e:
        logger.error('Failed to fetch index! {%s}' % e)
        # raise fetchError('Fetching failed')


def get_daily_adjusted(config,ticker, type, today_only, index_name):
    key = config.TS_KEY
    pro = ts.pro_api(key)
    for _ in range(3):
        time.sleep(30)
        try:
            df = pro.query('daily', ts_code=ticker)
            df.drop(["pre_close","change","pct_chg","amount"], axis=1, inplace=True)
            df.rename(columns={"ts_code": "symbol", "trade_date": "date", "vol": "volume"}, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            if today_only:
                return df.loc[df.index.min()].to_frame().T # the latest quote
            return df
        except:
            # logger.error('Failed to fetch %s' % ticker)
            raise fetchError('Fetching failed')


class fetchError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def get_yahoo_finance_price(ticker, today_only):
    if 'SH' in ticker:
        url = 'https://finance.yahoo.com/quote/'+ticker.replace('SH','SS')+'/history?p='+ticker.replace('SH','SS')
    else:
        url = 'https://finance.yahoo.com/quote/'+ticker+'/history?p='+ticker
    try:
        html = requests.get(url, headers=_headers()).text
    except:
        time.sleep(2)
        html = requests.get(url, headers=_headers()).text
    try:
        soup = BeautifulSoup(html,'html.parser')
        soup_script = soup.find("script",text=re.compile("root.App.main")).text
        matched = re.search("root.App.main\s+=\s+(\{.*\})",soup_script)
        if matched:
            json_script = json.loads(matched.group(1))
            if today_only:
                data = json_script['context']['dispatcher']['stores']['HistoricalPriceStore']['prices'][0]
                df = pd.DataFrame({'date': dt.fromtimestamp(data['date']).strftime("%Y-%m-%d"),
                                 'close': round(data['close'], 2),
                                 'volume': int(str(data['volume'])[:-2]),
                                 'open': round(data['open'], 2),
                                 'high': round(data['high'], 2),
                                 'low': round(data['low'], 2),
                                 }, index=[0])
                return df
            else:
                data = json_script['context']['dispatcher']['stores']['HistoricalPriceStore']['prices']
                df = pd.DataFrame(data, columns=['date', 'close', 'volume', 'open', 'high', 'low'])
                df['date']  = df['date'].apply(lambda x: dt.fromtimestamp(x).strftime("%Y-%m-%d")).dropna()
                df['volume'] = df['volume'] // 100
                return df
    except Exception as e:
        raise fetchError('Fetching failed')

######################################## YAHOO Fetching #########



def get_yahoo_bvps(ticker):
    url = 'https://finance.yahoo.com/quote/{0}/key-statistics?p={0}'.format(ticker)
    try:
        html = requests.get(url, headers=_headers()).text
    except:
        time.sleep(30)
        html = requests.get(url, headers=_headers()).text
    try:
        soup = BeautifulSoup(html,'html.parser')
        soup_script = soup.find("script",text=re.compile("root.App.main")).text
        matched = re.search("root.App.main\s+=\s+(\{.*\})",soup_script)
        if matched:
            json_script = json.loads(matched.group(1))
            cp = json_script['context']['dispatcher']['stores']['QuoteSummaryStore']['defaultKeyStatistics']['bookValue']['fmt']
            return float(cp)
        else:
            return None
    except:
        return None


def get_yahoo_cr(ticker):
    url = 'https://finance.yahoo.com/quote/{0}/key-statistics?p={0}'.format(ticker)
    try:
        html = requests.get(url, headers=_headers()).text
    except:
        time.sleep(30)
        html = requests.get(url, headers=_headers()).text
    try:
        soup = BeautifulSoup(html,'html.parser')
        soup_script = soup.find("script",text=re.compile("root.App.main")).text
        matched = re.search("root.App.main\s+=\s+(\{.*\})",soup_script)
        if matched:
            json_script = json.loads(matched.group(1))
            cr = json_script['context']['dispatcher']['stores']['QuoteSummaryStore']['financialData']['currentRatio']['fmt']
            return float(cr)
        else:
            return None
    except:
        pass

def _headers():
    return {"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7",
            "cache-control": "max-age=0",
            "dnt": "1",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36"}
