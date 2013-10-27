"""
Job class

the basic element for the scheduler
"""
import os
import threading
import datetime
from util import now, GLOBAL_VARS, shell_exec
from logger import logger
from depend import JobDep
from dbadapter import db


class Job(object):
    """ a periodic job """
    def __init__(self):
        # meta information
        self.name = ''
        self.command = ''
        self.deps = []
        self.dep_expr = ''
        self.raw_content = None
        self.callback = None

        # periodic information
        self.at_time = None  # optional time at which this job runs
        self.last_run = None  # datetime of the last run
        self.next_run = None  # datetime of the next run
        self.period = None  # timedelta between runs
        #self.freeze = False # suspend the job if freeze is True
        self.run_count = 0
        self.valid_window = ()
        self.message = ''

    def __lt__(self, other):
        """ jobs are sortable based on the scheduled time they run next."""
        return self.next_run < other.next_run

    @property
    def is_valid(self):
        """ check the job status accroding to the different conditions """
        return self.limit_rule and self.window_rule and self.dep_rule

    @property
    def limit_rule(self):
        limit_rule = (self.run_limit < 0) or \
            (self.run_limit >= 0 and self.run_count < self.run_limit)
        self.message += 'limit_check: (cnt=%d, limit=%d) => %s\n' % (self.run_count, self.run_limit, limit_rule)
        return limit_rule

    @property
    def window_rule(self):
        window_rule = self.valid_window[0] <= now() <= self.valid_window[1] \
                     if len(self.valid_window) == 2 else True
        self.message += 'window_check: (valid_window=%s, runtime=%s) => %s\n' % (self.valid_window, now(), window_rule)
        return window_rule

    @property
    def dep_rule(self):
        """ evaluate the dependencies.
        support self defined evaluation relationship. """
        # TODO: if the job dep is not satisfied, freeze the job for sometime
        if len(self.deps) == 0:
            return True
        vals = []
        for dep in self.deps:
            if isinstance(dep, JobDep):
                vals.append(dep.eval())
            else:
                raise Exception('%s is not instance of JobDep', dep)
        cmd = self.dep_expr % tuple(vals)
        ret = eval(cmd)
        self.message += 'dependency_rule: %s => %s\n' % (cmd, ret)
        return ret

    @property
    def time_rule(self):
       time_rule = (self.next_run and now() >= self.next_run)
       self.message += 'runtime_check: (next_run=%s, runtime=%s) => %s\n' % (self.next_run, now(), time_rule)
       return time_rule

    @property
    def should_run(self):
        """ true if the job should be run now."""
        self.message = 'job: %s\n' % self.name
        final_check = self.is_valid and self.time_rule
        self.message += 'final ==> %s' % final_check
        logger.debug(self.message)
        return final_check

    def need_gc(self):
        # maybe not accurate, as if the job is a periodic job,
        # it probaly will expire at a specular running
        return not self.limit_rule or not self.window_rule

    def set_frequency(self, interval=1, unit='days', limit=-1):
        self.unit = unit
        self.interval = interval
        self.run_limit = limit

    def instantiate(self, callback=None):
        """ kickoff the job running.
        it is similar to create an instant from a class. """
        # init the control variables
        self.run_count = 0
        self.last_run = None
        self.next_run = None
        self.valid_window = \
            (now(), now() + (self.valid_window[1] - self.valid_window[0])) \
            if len(self.valid_window) == 2 else ()
        # evaluate the global variables
        for args in GLOBAL_VARS.items():
            self.command = self.command.replace(args[0], args[1]())

        def fn():
            (ret, out) = shell_exec(self.command)
            [cb(ret, out, self) for cb in callbacks if cb]
        self.job_func = fn
        callbacks = [self.callback, callback]
        self._schedule_next_run()

    def at(self, time_str):
        """Schedule the job every day at a specific time.
        Calling this is only valid for jobs scheduled to run every N day(s).
        """
        assert self.unit == 'days'
        hour, minute = [int(t) for t in time_str.split(':')]
        assert 0 <= hour <= 23
        assert 0 <= minute <= 59
        self.at_time = datetime.time(hour, minute)
        return self

    def do(self, job_func, *args, **kwargs):
        """Specifies the job_func that should be called every time the job runs.
        Any additional arguments are passed on to job_func when the job runs.
        """
        self.job_func = functools.partial(job_func, *args, **kwargs)
        functools.update_wrapper(self.job_func, job_func)
        self._schedule_next_run()
        return self

    def run(self, is_parallel):
        """ run the job and immediately reschedule it."""
        self.run_count += 1
        self.last_run = now()
        if is_parallel:
            self.thread = threading.Thread(target=self.job_func)
            self.thread.start()
            logger.info('Running job thread: %s', self.thread)
            ret = True
        else:
            ret = self.job_func()
        self._schedule_next_run()
        return ret

    def _schedule_next_run(self):
        """ compute the instant when this job should run next."""
        assert self.unit in ('seconds', 'minutes', 'hours', 'days', 'weeks')
        self.period = datetime.timedelta(**{self.unit: self.interval})
        self.next_run = now() + self.period
        if self.at_time:
            #assert self.unit == 'days'
            self.next_run = self.next_run.replace(hour=self.at_time.hour,
                                                  minute=self.at_time.minute,
                                                  second=self.at_time.second,
                                                  microsecond=0)
            # If we are running for the first time, make sure we run
            # at the specified time *today* as well
            #if (not self.last_run and
                #self.at_time > now().time()):
                #self.next_run = self.next_run - datetime.timedelta(days=1)

    def job_meta(self):
        """ return the job meta data """
        return {
            'name': self.name,
            'command': self.command,
            'raw_content': self.raw_content,
            'frequency': '%s%s' % (self.interval,self.unit),
            'at_time': str(self.at_time),
            'valid_window': str(self.valid_window)
        }

    def runtime_info(self):
        """ return the job runtime information """
        full_dep = ';'.join(['%s' % d for d in self.deps]) \
                   if len(self.deps) > 0 else ''
        return {
            'job_name': self.name,
            'full_name': '%s-%s' % (self.name, self.next_run),
            'command': self.command, # TODO: cmd -> raw_cmd + rt_cmd
            'full_dep': full_dep,
            'status': 'Unknown',
            'owner': 'System',
            'trigger_mode': 'Manual',
            'start_time': '',
            'stop_time': '',
            'message': self.message
        }

