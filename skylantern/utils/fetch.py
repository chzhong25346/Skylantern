from datetime import datetime as dt
import pandas as pd
import json
import time
import os
import tushare as ts
import logging
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
