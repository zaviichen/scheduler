#!/usr/bin/python
"""
the schedule server interfaces for users
"""

import sys
import os
import pwd
import httplib
import urllib
import urllib2

#addr = 'localhost:8080'
#addr = '10.15.177.211:8080'
addr = 'localhost:7751'
user = None

def print_tree(parent, tree, indent='', stat={}):
    print '%s %s' % (parent, stat.get(parent, ''))
    if parent not in tree:
        return
    for child in tree[parent][:-1]:
        sys.stdout.write(indent + '|--')
        print_tree(child, tree, indent + '|  ', stat)
    child = tree[parent][-1]
    sys.stdout.write(indent + '`--')
    print_tree(child, tree, indent + '    ', stat)

def post(addr, context, act='job'):
    params = urllib.urlencode(context)
    headers = {'Content-type': 'application/x-www-form-urlencoded',
        'Accept': 'text/plain'}
    conn = httplib.HTTPConnection(addr)
    conn.request('POST', '/'+act, params, headers)
    # TODO: conn.close
    return conn.getresponse()

def get(addr, context, act='control'):
    params = '&'.join('%s=%s' % t for t in context.items())
    url = 'http://%s/%s?%s' % (addr, act, params)
    rsp = urllib2.urlopen(url)
    return rsp.read()

#
# commands
#
def add_job(context):
    """ sendevent add_job -f file [-n runtimes] """
    params = {'action':'add'}
    try:
        file = context['-f']
        content = open(file,'r').read()
        params['file'] = file
        params['content'] = content
    except Exception, e:
        print e
    post(addr, params)

def remove_job(context):
    """ sendevent remove_job -n name """
    params = {'action':'remove'}
    params['job'] = context['-n']
    post(addr, params)

def trigger_job(context):
    """ sendevent trigger_job -n name """
    params = {'action':'trigger_job','user':user}
    params['name'] = context['-n']
    print get(addr, params, 'job')

def start(context):
    """ sendevent start """
    params = {'action':'start'}
    get(addr, params)

def stop(context):
    """ sendevent stop """
    params = {'action':'stop'}
    get(addr, params)

def play(context):
    """ sendevent play """
    params = {'action':'play'}
    get(addr, params)

def pause(context):
    """ sendevent pause """
    params = {'action':'pause'}
    get(addr, params)

def job_info(context):
    """ show job meta information """
    pass

def job_status(context):
    """ sendevent job_status -n name """
    print _job_status(context)

def _job_status(context):
    """ sendevent job_status -n name """
    params = {'action':'job_status'}
    params['name'] = context['-n']
    return get(addr, params, 'job')

def job_statuses(context):
    """ sendevent job_statuses -n name """
    dep_tree = _job_dep(context)
    name = context['-n']
    stat_dic = {}
    for dep in dep_tree.keys():
        st = _job_status({'-n':dep})
        stat_dic[dep] = '(%s)' % st
    print_tree(name, dep_tree, stat=stat_dic)
    return (dep_tree, stat_dic)

def job_message(context):
    """ sendevent job_message -n name """
    params = {'action':'job_message'}
    params['name'] = context['-n']
    print get(addr, params, 'job')

def job_dep(context):
    """ sendevent job_dep -n name [-l level] """
    dep_tree = _job_dep(context)
    print_tree(context['-n'], dep_tree)

def _job_dep(context):
    params = {'action':'job_dep'}
    params['name'] =  context['-n']
    params['level'] = context.get('-l', 1)
    dep_tree = eval(get(addr, params, 'job'))
    return dep_tree

def all_jobs(context):
    """ sendevent all_jobs """
    params = {'action':'all_jobs'}
    ret = get(addr, params, 'job')
    print ret
    return ret

def runtime_infos(context):
    """ sendevent runtime_infos """
    params = {'action':'runtime_infos'}
    params['infos'] = '[]'
    ret = get(addr, params, 'job')
    print ret
    return ret


if __name__ == '__main__':
    argc = len(sys.argv)
    if argc < 1 and argc % 2 != 0:
        print 'usage: sendevent.py cmd -h'
        sys.exit()

    user = pwd.getpwuid(os.getuid())[0]
    context = {}
    if argc > 3:
        context = dict(zip(sys.argv[2::2], sys.argv[3::2]))
    try:
        thismodule = sys.modules[__name__]
        func = getattr(thismodule, sys.argv[1])
        func(context)
    except Exception,e:
        print e

