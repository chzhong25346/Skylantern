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
        opts, args = getopt.getopt(argv,"u",["update="])
    except getopt.GetoptError:
        print('run.py -u <full|compact|fastfix|slowfix> <CSI300>')
        sys.exit(2)
        
    for opt, arg in opts:
        if opt == '-h':
            print('run.py -u <full|compact|fastfix|slowfix>  <CSI300>')
            sys.exit()
        elif (opt == '-u' and len(argv) != 3):
            print('run.py -u <full|compact|fastfix|slowfix  <CSI300>')
            sys.exit()
        elif opt in ("-u", "--update"):
            if(arg == 'full'):
                pass

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
        # Fetch/Mapping/Write Index
        bulk_save(s, map_index(index_name))
    tickerL = read_ticker(s)

    if (fix == 'slowfix'):
        pass

    for ticker in tickerL:
    # for ticker in ['AMD']: # Fast fix a ticker
        try:
            if (fix == 'fastfix'): # Fast Update, bulk
                pass
            elif (fix == 'slowfix'): # Slow Update, one by one based on log.log
                pass
            else: # Compact Update
                pass

        except writeError as e:
            logger.error("%s - (%s,%s)" % (e.value, index_name, ticker))
        except foundDup as e:
            logger.error("%s - (%s,%s)" % (e.value, index_name, ticker))
        except fetchError as e:
            logger.error("%s - (%s,%s)" % (e.value, index_name, ticker))
        except:
            logger.error("Updating failed - (index_name,%s)" % (index_name,ticker))
    s.close()
