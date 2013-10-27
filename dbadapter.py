import sqlite3
import MySQLdb
import re
import time
import threading
from util import Singleton, now
from logger import logger
from config import config

CREATE_META_TABLE = """
create table job_meta (
    name text,
    command text,
    frequency text,
    at_time text,
    valid_window text,
    raw_content text,
    update_time text
);
"""

CREATE_RUNTIME_TABLE = """
create table runtime_info (
    job_name text,
    full_name text,
    command text,
    full_dep text,
    status text,
    owner text,
    trigger_mode text,
    start_time text,
    stop_time text,
    message text
);
"""

DROP_META_TABLE = """
drop table job_meta;
"""

DROP_RUNTIME_TABLE = """
drop table runtime_info;
"""

REGISTER_JOB = """
insert into job_meta values(
    '%(name)s',
    '%(command)s',
    '%(frequency)s',
    '%(at_time)s',
    '%(valid_window)s',
    '%(raw_content)s',
    '%(update_time)s'
);
"""

DELETE_JOB = """
delete from job_meta where name='%s'
"""

INSERT_RUNTIME = """
insert into runtime_info values(
    '%(job_name)s',
    '%(full_name)s',
    '%(command)s',
    '%(full_dep)s',
    '%(status)s',
    '%(owner)s',
    '%(trigger_mode)s',
    '%(start_time)s',
    '%(stop_time)s',
    '%(message)s'
);
"""

DELETE_RUNTIME = """
delete from runtime_info where job_name='%s'
"""

UPDATE_RUNTIME = """
update runtime_info set %s where %s;
"""

QUERY_RUNTIME = """
select %s from runtime_info where %s;
"""

def box_sql(str):
    str = 'null' if str is None else str
    str = str.replace("'","''")
    return str

def unbox_sql(str):
    str = str.replace("''","'")
    str = None if str=='null' else str
    return str

def box_dic(dic):
    return dict((k, box_sql(str(v))) for (k,v) in dic.items())


class DbAdapter():
    """ """
    __metaclass__ = Singleton

    def __init__(self):
        self.lock  = threading.Lock()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def execute(self, sql):
        self.lock.acquire()
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except:
            try:
                time.sleep(1)
                self.cur.execute(sql)
                self.conn.commit()
            except Exception, e:
                logger.error(e)
        finally:
            self.lock.release()

    def create_tables(self):
        self.execute(CREATE_META_TABLE)
        self.execute(CREATE_RUNTIME_TABLE)

    def drop_tables(self):
        self.execute(DROP_META_TABLE)
        self.execute(DROP_RUNTIME_TABLE)

    def update_meta(self, dic):
        self.delete_meta(dic['name'])
        dic['update_time'] = now()
        self.execute(REGISTER_JOB % box_dic(dic))

    def delete_meta(self, name):
        self.execute(DELETE_JOB % name)

    def insert_runtime(self, dic):
        self.delete_runtime(dic['job_name'])
        self.execute(INSERT_RUNTIME % box_dic(dic))

    def delete_runtime(self, name):
        self.execute(DELETE_RUNTIME % name)

    def update_runtime(self, val_dic, cond_dic):
        val = ','.join("%s='%s'" % t for t in box_dic(val_dic).items())
        cond = ' and '.join("%s='%s'" % t for t in box_dic(cond_dic).items())
        self.execute(UPDATE_RUNTIME % (val, cond))

    def query_runtime(self, vals, cond_dic):
        val = '*' if len(vals)==0 else ','.join(vals)
        cond = ' and '.join("%s='%s'" % t for t in box_dic(cond_dic).items())
        cmd = QUERY_RUNTIME % (val, cond)
        logger.debug('[SQL] %s' % cmd.strip())
        self.execute(QUERY_RUNTIME % (val, cond))
        return self.cur.fetchone()


class SqliteAdapter(DbAdapter):
    """ Python bulid-in sqlite3 adapter """
    def __init__(self):
        super(SqliteAdapter, self).__init__()
        elem = config.get_elem('sqlite')
        get = lambda t: config.get_text(elem,t)
        self.conn = sqlite3.connect(
            database = get('name'),
            check_same_thread = get('check_same_thread').lower() == 'true',
            timeout = int(get('timeout'))
        )
        self.cur = self.conn.cursor()


class MysqlAdapter(DbAdapter):
    """ Mysql adapter """
    def __init__(self):
        super(MysqlAdapter, self).__init__()
        elem = config.get_elem('mysql')
        get = lambda t: config.get_text(elem,t)
        self.conn = MySQLdb.connect(
            host = get('host'),
            user = get('user'),
            passwd = get('passwd'),
            db = get('name'),
            port = int(get('port'))
        )
        self.cur = self.conn.cursor()


select = config.get_attr(config.get_elem('db'), 'select')
db = DbAdapter()

if select == 'mysql':
    db = MysqlAdapter()
elif select == 'sqlite':
    db = SqliteAdapter()


if __name__ == '__main__':
    #sa = SqliteAdapter()
    sa = MysqlAdapter()
    sa.create_tables()
