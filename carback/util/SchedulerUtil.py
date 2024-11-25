from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler

from config import Config


class SchedulerUtil(object):
    scheduler=BackgroundScheduler(timezone='Asia/Shanghai',executors={
            'default':ThreadPoolExecutor(10),
            'processpool':ProcessPoolExecutor(5)
        })
    scheduler.add_jobstore('sqlalchemy',url=Config.SQLALCHEMY_DATABASE_URI)
    @staticmethod
    def start(*args, **kwargs):
        SchedulerUtil.scheduler.start(*args, **kwargs)
    @staticmethod
    def stop(job_id):
        SchedulerUtil.scheduler.pause_job(job_id)

    @staticmethod
    def add_job(*args,**kwargs):
        SchedulerUtil.scheduler.add_job(*args,**kwargs)

    @staticmethod
    def remove_job(job_id):
        SchedulerUtil.scheduler.remove_job(job_id)

    @staticmethod
    def remove_all_jobs():
        SchedulerUtil.scheduler.remove_all_jobs()