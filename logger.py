"""
"""
import os
import logging
import datetime

log_format = '%(asctime)s [%(levelname)s]: %(message)s'
log_file = 'log.%s' % datetime.datetime.now().strftime('%Y-%m-%d')

logging.basicConfig(filename = os.path.join(os.getcwd(), log_file),
                    level = logging.DEBUG,
                    format = log_format)

# make the log to multi-destinations
# both console and file
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(log_format))
logging.getLogger('').addHandler(console)

logger = logging.getLogger('schduler')

