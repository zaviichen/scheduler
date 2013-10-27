"""
"""
import json
import pwd
import os
from util import now

class Request(object):
    """ """
    def __init__(self, action=None, params=None, other=None):
        self.user = pwd.getpwuid(os.getuid())[0]
        self.action = action
        self.params = params
        self.other = other
        self.id = '%s_%s_%s' % (self.user, self.action, now())

    @staticmethod
    def create(json_str):
        req = None
        try:
            data = json.loads(json_str)
            req = Request()
            req.action = data.get('action', None)
            req.params = data.get('params', None)
            req.other = data.get('other', None)
            req.id= data.get('id', None)
        except:
            pass
        return req

    def __str__(self):
        data = {
            'user': self.user,
            'action': self.action,
            'params': self.params,
            'other': self.other,
            'id': self.id
        }
        return json.dumps(data)


class Response(object):
    """ """
    def __init__(self, id=None, stat='success', msg=None, rsp=None):
        self.id = id
        self.status = stat
        self.message = msg
        self.response = rsp

    @staticmethod
    def create(json_str):
        rsp = None
        try:
            data = json.loads(json_str)
            rsp = Response()
            rsp.id = data.get('id', None)
            rsp.status = data.get('status','success')
            rsp.message = data.get('message', None)
            rsp.response = data.get('response', None)
        except:
            pass
        return rsp

    def __str__(self):
        data = {
            'id': self.id,
            'status': self.status,
            'message': self.message,
            'response': self.response
        }
        return json.dumps(data)
