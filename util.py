"""
"""
import os
import sys
import threading
from hdfs import exists as hexists
from hdfs import touchz
import datetime
import commands

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def exists(file, hdfs=False):
    return os.path.exists(file) if not hdfs else hexists(file)

def touch(file, hdfs=False):
    if not hdfs:
        open(file, 'w').close()
    else:
        touchz(file)

def now():
    return datetime.datetime.now()

def format(format='%Y-%m-%d %H:%M:%S'):
    return datetime.datetime.now().strftime(format)

def format_date(date, format='%Y-%m-%d'):
    return date.strftime(format)

def thread_id():
    return threading.currentThread().ident

def shell_exec(cmd):
    def _system(cmd_):
        status, output = os.system(cmd_), ""
        return (status, output)

    def _commands(cmd_):
        import commands
        return commands.getstatusoutput(cmd_)

    def _subprocess(cmd_):
        import subprocess
        _file = 'tmpout.%s' % thread_id()
        fw = open(_file, 'w')
        status = subprocess.call(cmd_, shell=True, stdout=fw, stderr=fw)
        fw.close()
        fr = open(_file, 'r')
        output = fr.read()
        fr.close()
        os.remove(_file)
        return (status, output)
    #return _subprocess(cmd)
    #return _commands(cmd)
    return _system(cmd)


GLOBAL_VARS = {
    '${date}':  lambda: '%02d' % now().day,
    '${hour}':  lambda: '%02d' % now().hour,
    '${min}':   lambda: '%02d' % now().minute,
    '$$':       lambda: str(threading.currentThread().ident)
}

