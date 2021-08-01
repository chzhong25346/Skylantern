import pandas as pd
import datetime as dt
from ..utils.fetch import fetch_index#, get_daily_adjusted
from ..utils.util import gen_id
from ..models import Index, Quote, Report, Transaction, Holding, Eia_price, Eia_storage
import logging
logger = logging.getLogger('main.mapping')


def map_index(index_name):
    df = fetch_index(index_name)
    df_records = df.to_dict('records')
    model_instnaces = [Index(
        symbol = record['symbol'],
        company = record['company'],
    ) for record in df_records]

    return model_instnaces


def map_quote(df, ticker):
    df_records = df.to_dict('records')
    try:
        model_instnaces = [Quote(
            id = gen_id(ticker+str(record['date'])),

            symbol = ticker,
            date = record['date'],
            open = record['open'],
            high = record['high'],
            low = record['low'],
            close = record['close'],
            volume = record['volume']
        ) for record in df_records]

        return model_instnaces
    except TypeError:
        model_instance = Quote(
            id = gen_id(ticker+str(dt.datetime.strptime(record['date'], "%Y-%m-%d"))),
            symbol = ticker,
            date = record['date'],
            open = record['open'],
            high = record['high'],
            low = record['low'],
            close = record['close'],
            volume = record['volume']
            )
        return model_instance
    except:
        raise mappingError('Mapping failed')


def map_fix_quote(sr, ticker):
    try:
        model_instance = Quote(
            id = gen_id(ticker+str(dt.datetime.strptime(sr['date'], "%Y-%m-%d"))),
            symbol = ticker,
            date = sr['date'],
            open = sr['open'],
            high = sr['high'],
            low = sr['low'],
            close = sr['close'],
            volume = sr['volume']
            )
        return model_instance
    except TypeError:
        model_instance = Quote(
            id = gen_id(ticker+str(sr['date'])),
            symbol = ticker,
            date = sr['date'],
            open = sr['open'],
            high = sr['high'],
            low = sr['low'],
            close = sr['close'],
            volume = sr['volume']
            )
        return model_instance
    except:
        raise mappingError('Mapping failed')


def map_report(config,df):
    date = dt.datetime.today().strftime("%Y-%m-%d")
    df_records = df.to_dict('records')
    model_instnaces = [Report(
        symbol = record['symbol'],
        date = date,
        id = gen_id(record['symbol']+str(date)),
        yr_high = record['yr_high'],
        yr_low = record['yr_low'],
        downtrend = record['downtrend'],
        uptrend = record['uptrend'],
        high_volume = record['high_volume'],
        rsi = record['rsi'],
        macd = record['macd'],
        bolling = record['bolling'],
        # adx = record['adx'],
    ) for record in df_records]
    logger.info('Mapping completed.')

    return model_instnaces


def map_transaction(df):
    date = dt.datetime.today().strftime("%Y-%m-%d")
    df_records = df.to_dict('records')
    model_instnaces = [Transaction(
        id = gen_id(record['symbol'] + record['type'] + str(date)),
        date = date,
        symbol = record['symbol'],
        price = record['price'],
        quantity = record['quantity'],
        settlement = record['settlement'],
        type = record['type'],
    ) for record in df_records]
    logger.info('Mapping completed.')

    return model_instnaces


def map_holding(df):
    df_records = df.to_dict('records')
    model_instnaces = [Holding(
        symbol = record['symbol'],
        avg_cost  = record['avg_cost'],
        quantity = record['quantity'],
        book_value  = record['book_value'],
        change_dollar  = record['change_dollar'],
        change_percent  = record['change_percent'],
        mkt_price  = record['mkt_price'],
        mkt_value  = record['mkt_value'],
        note  = record['note'],
    ) for record in df_records]
    logger.info('Mapping completed.')

    return model_instnaces


# Eia_price
def map_eia_price(df, sid):
    df_records = df.to_dict('records')
    model_instnaces = [Eia_price(
        id = gen_id(sid + str(record['date'])),
        sid = sid,
        date = record['date'],
        value = record['value'],
    ) for record in df_records]
    logger.info('Mapping completed.')

    return model_instnaces


# Eia_price
def map_eia_storage(df, sid):
    df_records = df.to_dict('records')
    model_instnaces = [Eia_storage(
        id = gen_id(sid + str(record['date'])),
        sid = sid,
        date = record['date'],
        value = record['value'],
    ) for record in df_records]
    logger.info('Mapping completed.')

    return model_instnaces


class mappingError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
