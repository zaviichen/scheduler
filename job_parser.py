"""
"""
import re
import xml.etree.ElementTree as ET
from util import GLOBAL_VARS
from job import Job

class JobParser():
    """ parser for xml defined job description """
    def __init__(self):
        pass

    def parse(self, conf):
        """ """
        # add the brackets between the variables
        _trim = lambda s: re.sub(r'\${?(\w+)}?', r'${\1}', s)

        # step1: replace the variables, except the GLOBAL_VARS
        try:
            content = _trim(open(conf, 'r').read())
            root = ET.fromstring(content)
            params = self._eval_params(root)
            content = self._eval_string(content, params)
            # re-parse the xml after the variable substitution
            root = ET.fromstring(content)
        except Exception, e:
            print e

        # step2: parse the config to generate the job
        job = Job()
        try:
            job.name = root.find('name').text
            job.command = root.find('command').text
        except Exception, e:
            print e
        return job

    def _eval_string(self, str, params):
        """ """
        eval = lambda s,k,v: re.sub('\${%s}'%k,v,s)
        def get_value(key):
            for pair in params:
                if pair[0] == key:
                    return pair[1]
            return None

        for var in re.findall('\${(\w+)}',str):
            val = get_value(var)
            if val:
                str = eval(str, var, val)
            else:
                if '${%s}' % var not in GLOBAL_VARS:
                    print 'error: can not find the variable (%s)' % var
        return str

    def _eval_params(self, root):
        """ Evaluate all the variables.
        Keep sure the variable is defined before it is used. """
        params = [[param.attrib['name'], param.attrib['value']]
                for param in root.iter('param')]
        # It will update the 'params' in the real time.
        # Do not use list comprehension.
        for p in params:
            p[1] = self._eval_string(p[1], params)
        return params

    def _eval_command(self):
        """ """
        pass
        #self.command = self._eval_string(self.command)

