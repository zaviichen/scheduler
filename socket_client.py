"""
"""
import socket
import sys
from socket_header import Request, Response

HOST, PORT = "localhost", 9999

def send(req):
    rsp = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        sock.sendall(str(req))
        rsp_str = sock.recv(102400).strip()
        rsp = Response.create(rsp_str)
    except Exception,e:
        rsp = Response(req.id, 'fail', str(e))
    finally:
        sock.close()
    return rsp

# interface
def sendevent(api, params):
    thismodule = sys.modules[__name__]
    func = getattr(thismodule, api)
    return str(func(params))

# administor commands
def start(params):
    """ """
    return send(Request('start'))

def stop(params):
    """ """
    return send(Request('stop'))

def play(params):
    """ """
    return send(Request('play'))

def pause(params):
    """ """
    return send(Request('pause'))

# job control commands
def add_job(params):
    """ """
    try:
        file = params['-j']
        content = open(file,'r').read()
        params['file'] = file
        params['content'] = content
    except Exception, e:
        print e
    return send(Request('add_job', params))

def runtime_infos(params):
    """ """
    return send(Request('runtime_infos'))

def job_dep(params):
    """ """
    params['name'] = params['-j']
    params['level'] = params.get('-l', 1)
    return send(Request('job_dep', params))


if __name__ == '__main__':
    import sys
    argc = len(sys.argv)
    if argc < 1 and argc % 2 != 0:
        print 'usage: sendevent.py cmd -h'
        sys.exit()

    context = {}
    if argc > 3:
        context = dict(zip(sys.argv[2::2], sys.argv[3::2]))
    try:
        thismodule = sys.modules[__name__]
        func = getattr(thismodule, sys.argv[1])
        print func(context)
    except Exception,e:
        print e
