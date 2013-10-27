"""
"""
import json
import SocketServer
from schedule import Scheduler
from logger import logger
from socket_header import Request, Response

scheduler = None

class SchedulerHandler(SocketServer.BaseRequestHandler):
    """ """
    def recv_basic(self):
        total_data = []
        while True:
            data = self.request.recv(1024).strip()
            if not data: break
            total_data.append(data)
        return ''.join(total_data)

    def handle(self):
        req_str = self.request.recv(102400).strip()
        req = Request.create(req_str)
        logger.info('connect from %s' % self.client_address[0])
        logger.info('[JSON] request:  %s' % req)
        rsp = self.dispatcher(req)
        logger.info('[JSON] response: %s' % rsp)
        self.request.sendall(str(rsp))

    def dispatcher(self, req):
        rsp = Response(req.id)
        if req.action:
            try:
                rsp.response = getattr(self, req.action)(req.params)
            except Exception,e:
                rsp.status = 'fail'
                rsp.message = str(e)
        else:
            rsp.status = 'fail'
            rsp.message = 'fail to find the action: %s' % action
        return rsp

    #
    # APIs
    #
    def start(self, params):
        return scheduler.start()

    def stop(self, params):
        return scheduler.stop()

    def play(self, params):
        return scheduler.play()

    def pause(self, params):
        return scheduler.pause()

    # job control apis
    def runtime_infos(self, params):
        return scheduler.runtime_infos(params)

    def add_job(self, params):
        job = scheduler.add_job(params)
        return scheduler.instantiate(job)

    def remove_job(self, params):
        return scheduler.remove_job(params)

    def trigger_job(self, params):
        return scheduler.trigger_job(params)

    def job_status(self, params):
        return scheduler.job_status(params)

    def job_dep(self, params):
        return scheduler.job_dep(params)


class SchedulerServer(object):
    """ """
    def __init__(self, host, port):
        logger.info('socket server inits: %s:%s' % (HOST, PORT))
        self.server = SocketServer.TCPServer((host, port), SchedulerHandler)
        global scheduler
        scheduler = Scheduler()

    def serve_forever(self):
        logger.info('socket server starts')
        scheduler.start()
        self.server.serve_forever()

    def shutdown(self):
        logger.info('socket server stops')
        scheduler.stop()
        self.server.shutdown()


if __name__ == "__main__":
    import sys
    try:
        HOST, PORT = "localhost", 9999
        server = SchedulerServer(HOST, PORT)
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        sys.exit(0)
