"""
Scheduler config parser
"""
import xml.etree.ElementTree as ET
from logger import logger
from util import Singleton

class Config():
    """ scheduler config parser """
    __metaclass__ = Singleton

    def __init__(self, config):
        self.config = None
        try:
            tree = ET.parse(config)
            self.root = tree.getroot()
        except Exception, e:
            logger.error('parse config %s error: %s' % (config, e))

    def get_text(self, elem, tag):
        return elem.find(tag).text

    def get_attr(self, elem, attr):
        return elem.attrib.get(attr, None)

    def get_elem(self, tag):
        elems = list(self.root.getiterator(tag))
        return elems[0] if len(elems) > 0 else None

config = Config('config.xml')
