import logging
from eiapy import Series
from ..db.mapping import map_eia_price, map_eia_storage
from ..db.write import bulk_save, insert_onebyone, writeError, foundDup
import pandas as pd
import time
logger = logging.getLogger('main.learning')


def updateEIA(s):
    ## Storage
    _EIA_storage = [
        'PET.WGIRIUS2.W', # U.S. Gross Inputs into Refineries
        'PET.W_EPC0_IM0_NUS-NCA_MBBLD.W', # U.S. Imports from Canada of Crude Oil
        'PET.WCRIMUS2.W', # U.S. Imports of Crude Oil, Weekly
        'PET.WCRSTUS1.W', # U.S. Ending Stocks of Crude Oil, Weekly
        'PET.WCESTUS1.W', # U.S. Ending Stocks excluding SPR of Crude Oil, Weekly
        'PET.WCSSTUS1.W', # U.S. Ending Stocks of Crude Oil in SPR
        'PET.WGTSTUS1.W', # U.S. Ending Stocks of Total Gasoline, Weekly
        ]
    ## Prices
    _EIA_price = [
        'NG.RNGWHHD.D', # Natural Gas Spot and Futures Prices (NYMEX)
        'PET.RBRTE.D', # Europe Brent Spot Price FOB, Daily
        'PET.RWTC.D', # Cushing, OK WTI Spot Price FOB, Daily
        ]

    for sid in _EIA_price+_EIA_storage:
        time.sleep(5)
        try:
            df = fetchEIA(sid, update=True) # update=False for new SID Bulk update
            if not df.empty:
                if sid in _EIA_price:
                    # bulk_save(s, map_eia_price(df, sid))  # for new SID Bulk update
                    insert_onebyone(s, map_eia_price(df, sid))
                elif sid in _EIA_storage:
                    # bulk_save(s, map_eia_storage(df, sid)) # for new SID Bulk update
                    insert_onebyone(s, map_eia_storage(df, sid))
            logger.debug('(EIA %s) %s row(s) written' % ( sid, len(df.index) ))

        except (writeError, foundDup, fetchEIA_Error) as e:
            logger.error("%s - (Series ID: %s)" % (e.value, sid))
            pass
        except:
            pass


def fetchEIA(series_ID, update=True):
    try:
        sr = Series(series_ID)
        if update:
            data = sr.last(20)['series'][0]['data']
        else:
            data = sr.get_data(all_data=True)['series'][0]['data']
        df = pd.DataFrame.from_records(data, columns=['date','value'])
        df['date'] = pd.to_datetime(df['date'])
        df.dropna(inplace=True)
        return df
    except:
        raise fetchEIA_Error('Fetching EIA failed')


class fetchEIA_Error(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
