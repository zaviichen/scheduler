"""
"""
import re
import os
import functools
import schedule
from job import Job
from depend import JobDep
from logger import logger
from util import exists, now, touch, format_date, thread_id
from datetime import timedelta

# due to there is no time info in .job file
# so we set it to the default value
interval = 5
unit = 'seconds'
at_time_delay = timedelta(seconds=5)
valid_window_len = timedelta(days=1)
run_limit = 1

# define name variable
JOB_NAME = 'JOB_NAME'
COMMAND = 'JOB_COMMAND_LIST'
HDONE = 'JOB_HDONE'
LDONE = 'JOB_LDONE'
DEP_JOB = 'DEP_JOB_NAME_ARRAY'
DEP_HDONE = 'DEP_JOB_HDONE_ARRAY'
DEP_LDONE = 'DEP_JOB_LDONE_ARRAY'


class DotJobParser(object):
    """ parser for the .job file """
    def __init__(self):
        pass

    def parse(self, job_content):
        """ parse the string content to create job """
        sp = lambda s: (s.split('=',1)[0], s.split('=',1)[1].strip('"'))
        params = dict(sp(line)
                for line in job_content.split('\n')
                if '=' in line)
        job = self._gen_job(params)
        job.raw_content = job_content
        return job

    def parse_from_file(self, job_file):
        try:
            job = self.parse(open(job_file, 'r').read())
        except Exception,e:
            print e
        return job

    def _gen_job(self, params):
        job = Job()
        job.name = self._gen_name(params)
        job.command = self._gen_cmd(job, params)
        (job.deps, job.dep_expr) = self._gen_dep(job, params)
        job.set_frequency(interval, unit)
        job.at_time = (now() + at_time_delay).time()
        job.valid_window = (now(), now() + valid_window_len)
        job.run_limit = run_limit
        job.callback = self._gen_callback(job, params)
        return job

    def _gen_name(self, params):
        name = params[JOB_NAME]
        return self._lucky_guess(name, params, HDONE, LDONE)

    def _gen_dep(self, job, params):
        """ Parse the dependency according to the JOB_DEP_X_ARRAY/NAME. """
        deps1, expr1 = self._gen_dep_from_status(job, params)
        deps2, expr2 = self._gen_dep_from_done(job, params)
        if len(deps1) > 0 and len(deps2) > 0:
            expr = '(%s) or (%s)' % (expr1, expr2)
        elif len(deps1) > 0 and len(deps2) == 0:
            expr = expr1
        elif len(deps1) == 0 and len(deps2) > 0:
            expr = expr2
        else:
            expr = 'True'
        deps1.extend(deps2)
        return (deps1, expr)

    def _gen_cmd(self, job, params):
        script = 'shell_%s.%s' % (job.name, thread_id())
        templ = """cmd="%s"
eval $cmd
"""
        f = open(script, 'w')
        f.write(templ % params[COMMAND])
        f.close()
        log = '%s.log.%s' % (job.name, format_date(now()))
        # we can not remove it in the cmd
        # as it will return success to replace the actual cmd output
        cmd = 'chmod +x %(script)s; ./%(script)s > %(log)s 2>&1' % {'script':script, 'log':log}
        return cmd

    def _gen_dep_from_status(self, job, params):
        # query the dep job status
        deps = []
        for dep_job in params[DEP_JOB].split(';'):
            if self._is_null_or_empty(dep_job):
                continue
            name = self._lucky_guess(dep_job, params, DEP_HDONE, DEP_LDONE)
            fn = functools.partial(self._get_status, job.name, name)
            deps.append(JobDep(dep_job, fn))
        expr = ' and '.join(['%s' for d in deps])
        return (deps, expr)

    def _get_status(self, job_name, dep_name):
        ret = schedule.Scheduler().job_status({'name':dep_name})
        val = (ret and ret.lower() == 'success')
        logger.debug('current job(%s) check the dep job status: %s(%s) => %s' % (job_name, dep_name,ret,val))
        return val

    def _gen_dep_from_done(self, job, params):
        # if the dep job doesn't exist, check .done file
        deps = []
        def done_fn(tag, _hdfs):
            for done in params[tag].split(';'):
                if self._is_null_or_empty(done):
                    continue
                if done.endswith('done'):
                    fn = functools.partial(exists, done, hdfs=_hdfs)
                    deps.append(JobDep(done, fn))
        done_fn(DEP_HDONE,True)
        done_fn(DEP_LDONE,False)
        expr = ' and '.join(['%s' for d in deps])
        return (deps, expr)

    def _lucky_guess(self, name, params, *argc):
        # lucky guess the job name
        dtptn = r'\d{4}-\d{2}-\d{2}'
        # if the job name doesn't end with digits,
        # so it maybe daily job
        if name and not re.match('.*\d+$',name):
            for s in (params[argc[0]], params[argc[1]]):
                res = re.search('(%s)' % dtptn, s)
                if res:
                    '%s-%s' % (name, res.groups()[0])
        return name

    def _is_null_or_empty(self, str):
        return True if not str or str.lower() == 'null' else False

    def _gen_callback(self, job, params):
        hdone = params[HDONE]
        ldone = params[LDONE]
        # 1, remove the temp shell script
        def remove_script(*args):
            script = re.search('chmod \+x (.*);', job.command).groups()[0]
            os.remove(script)
        # 2, create .done files
        fns = []
        if not self._is_null_or_empty(hdone):
            fn = functools.partial(touch, hdone, hdfs=True)
            fns.append(fn)
        if not self._is_null_or_empty(ldone):
            fn = functools.partial(touch, ldone, hdfs=False)
            fns.append(fn)
        # 3, combine the callback
        def callback(*args):
            #remove_script(*args)
            [fn() for fn in fns]
        return callback


if __name__ == '__main__':
    from glob import glob
    parser = DotJobParser()
    for file in glob('./test/.*') + glob('./test/*'):
        print file
        job = parser.parse_from_file(file)
        print '==>', job.name
