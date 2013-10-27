#!/usr/bin/python
"""
"""

import web
import threading
from schedule import Scheduler

urls = (
    '/', 'index',
    '/job', 'job',
    '/control', 'control'
)

scheduler = Scheduler()

class index:
    def GET(self):
        return "welcome";

class job:
    def GET(self):
        req = web.input()
        act = req['action']
        return getattr(scheduler, act)(req)

    def POST(self):
        req = web.input()
        action = req['action']
        if action == 'add':
            job = scheduler.add_job(req)
            return scheduler.instantiate(job)
        elif action == 'remove':
            return scheduler.remove_job(req)

class control:
    def GET(self):
        req = web.input()
        act = req['action']
        return getattr(scheduler, act)()


def main():
    app = web.application(urls, globals())
    app.run()


if __name__ == '__main__':
    main()

