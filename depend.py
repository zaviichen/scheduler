"""
"""
import types
import functools
from logger import logger

class JobDep():
    """ job dependency class """
    def __init__(self, content, cond):
        self.dep_type = None
        self.name = None
        self.cond = cond
        self.content = content

    def __str__(self):
        return self.content

    def eval(self):
        t = type(self.cond)
        if t in (functools.partial, types.FunctionType, types.LambdaType):
            return self.cond()
        elif t == types.BooleanType:
            return self.cond
        else:
            raise Exception('%s is not support for job dependency', t)

