import os
from apscheduler.schedulers.background import BackgroundScheduler

from utils import CONFIG
from tasks.sync import sync_sql_total

def start_main():
    pass

def end_main():
    pass

def main_dag():
    scheduler = BackgroundScheduler({
        'apscheduler.jobstores.default': {
            'type': 'sqlalchemy',
            'url': 'duckdb://' + CONFIG.LOCAL_DB_PATH + '/scheduler.db'
        },
        'apscheduler.executors.default': {
            'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
            'max_workers': '20'
        },
        'apscheduler.executors.processpool': {
            'type': 'processpool',
            'max_workers': '5'
        },
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '30',
        'apscheduler.timezone': 'UTC',
    })
    
    start_job = scheduler.add_job(start_main, 'interval', seconds=2)
    end_job = scheduler.add_job(end_main, 'interval', seconds=2)