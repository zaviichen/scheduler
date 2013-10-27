#!/usr/bin/python

import os
import time
import datetime

def fn(s):
    print '[%s] %s' % (datetime.datetime.now(), s)
    os.system('python sendevent.py %s' % s)


def test_scheduler_control():
    fn('start')
    fn('add_job -f job1.job')
    time.sleep(3)
    fn('pause')
    time.sleep(3)
    fn('play')
    time.sleep(3)
    fn('add_job -f job2.job')
    time.sleep(3)
    fn('remove_job -n job2')
    time.sleep(5)
    fn('stop')

def test_trigger_job():
    fn('start')
    fn('add_job -f job1.job')
    fn('add_job -f job2.job')
    time.sleep(3)
    fn('job_status -n job1')
    time.sleep(1)
    fn('job_status -n job2')
    time.sleep(1)
    fn('job_dep -n job2 -l 3')
    time.sleep(10)
    fn('trigger_job -n job2')
    time.sleep(5)
    fn('job_status -n job1')
    fn('job_status -n job2')
    fn('stop')

def test_valid_window():
    fn('start')
    fn('add_job -f job1.job')
    fn('add_job -f job2.job')
    time.sleep(25)
    fn('stop')

def test_flow():
    fn('start')
    fn('add_job -f test/qf.freshness_hour.14.job')
    fn('add_job -f test/qf.qiyi_pop_hour.14.job')
    fn('add_job -f test/qf.baidu_pop.job')
    fn('add_job -f test/qf.agg_quality_signal_4rec_hour.14.job')
    time.sleep(5)
    fn('job_dep -n qf.freshness_hour.14')
    fn('job_dep -n qf.qiyi_pop_hour.14')
    fn('job_dep -n qf.baidu_pop')
    fn('job_dep -n qf.agg_quality_signal_4rec_hour.14 -l 5')
    fn('job_message -n qf.freshness_hour.14')
    fn('job_message -n qf.qiyi_pop_hour.14')
    fn('job_message -n qf.baidu_pop')
    fn('job_message -n qf.agg_quality_signal_4rec_hour.14')
    time.sleep(15)
    fn('stop')

def test_dep_status():
    fn('start')
    fn('add_job -f job1.job')
    fn('add_job -f job2.job')
    time.sleep(3)
    fn('job_status -n job1')
    time.sleep(1)
    fn('job_status -n job2')
    time.sleep(1)
    fn('job_dep -n job2 -l 3')
    time.sleep(10)
    fn('job_status -n job1')
    fn('job_status -n job2')
    fn('job_statuses -n job2')
    fn('stop')

#test_valid_window()
#test_flow()
#test_trigger_job()
test_dep_status()

