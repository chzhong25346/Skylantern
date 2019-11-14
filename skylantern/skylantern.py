from .utils.config import Config
from .utils.fetch import get_daily_adjusted, fetchError
from .utils.util import missing_ticker
from .db.db import Db
from .db.write import bulk_save, insert_onebyone, writeError, foundDup
from .db.mapping import map_index, map_quote, map_fix_quote#, map_report
from .db.read import read_ticker, has_index
import logging
import logging.config
import getopt
import time
import math
import os, sys
logging.config.fileConfig('skylantern/log/logging.conf')
logger = logging.getLogger('main')


def main(argv):
    time_start = time.time()
    try:
        opts, args = getopt.getopt(argv,"u:rse",["update=", "report=", "simulate=", "emailing="])
    except getopt.GetoptError:
        print('run.py -u <full|compact|fastfix|slowfix> <csi300>')
        print('run.py -r <csi300>')
        print('run.py -s <csi300>')
        print('run.py -e')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('run.py -u <full|compact|fastfix|slowfix>  <csi300>')
            print('run.py -r <csi300>')
            print('run.py -s <csi300>')
            print('run.py -e')
            sys.exit()
        elif (opt == '-u' and len(argv) != 3):
            print('run.py -u <full|compact|fastfix|slowfix>  <csi300>')
            sys.exit()
        elif opt in ("-u", "--update"):
            if(arg == 'full'):
                index_name = argv[2]
                type = arg
                today_only = False
                update(type, today_only, index_name)  # Full update
            elif(arg == 'compact'):
                index_name = argv[2]
                type = arg
                today_only = True
                update(type, today_only, index_name)  # Compact update for today
            elif(arg == 'slowfix'):
                index_name = argv[2]
                type = 'full' # fixing requires full data
                today_only = False
                update(type, today_only, index_name, fix='slowfix')  # Compact update for today
            elif(arg == 'fastfix'):
                index_name = argv[2]
                type = 'full' # fixing requires full data
                today_only = False
                update(type, today_only, index_name, fix='fastfix')  # Compact update for today


    elapsed = math.ceil((time.time() - time_start)/60)
    logger.info("%s took %d minutes to run" % ( (',').join(argv), elapsed ) )


def update(type, today_only, index_name, fix=False):
    logger.info('Run Task:[%s %s UPDATE]' % (index_name, type))
    Config.DB_NAME=index_name
    db = Db(Config)
    s = db.session()
    e = db.get_engine()
    # Create table based on Models
    db.create_all()
    if has_index(s) == None:
        bulk_save(s, map_index(index_name))

    tickerL = read_ticker(s)

    if (fix == 'slowfix'):
        tickerL = missing_ticker(index_name)

    for ticker in tickerL:
    # for ticker in ['000001.SZ']: # Fast fix a ticker
        try:
            if (fix == 'fastfix'): # Fast Update, bulk
                df = get_daily_adjusted(Config,ticker,type,today_only,index_name)
                model_list = []
                for index, row in df.iterrows():
                    model = map_fix_quote(row, ticker)
                    model_list.append(model)
                logger.info("--> %s" % ticker)
                bulk_save(s, model_list)
            elif (fix == 'slowfix'): # Slow Update, one by one based on log.log
                df = get_daily_adjusted(Config,ticker,type,today_only,index_name)
                model_list = []
                for index, row in df.iterrows():
                    model = map_fix_quote(row, ticker)
                    model_list.append(model)
                logger.info("--> %s" % ticker)
                insert_onebyone(s, model_list)
            else:
                df = get_daily_adjusted(Config,ticker,type,today_only,index_name)
                model_list = map_quote(df, ticker)
                bulk_save(s, model_list)
                logger.info("--> %s" % ticker)

        except writeError as e:
            logger.error("%s - (%s,%s)" % (e.value, index_name, ticker))
        except foundDup as e:
            logger.error("%s - (%s,%s)" % (e.value, index_name, ticker))
        except fetchError as e:
            logger.error("%s - (%s,%s)" % (e.value, index_name, ticker))
        except:
            logger.error("Updating failed - (index_name,%s)" % (index_name,ticker))
    s.close()
