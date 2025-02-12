
import os
import threading
import queue
import concurrent.futures
from utils.config import CONFIG

# 仅在类型检查时导入
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from tasks.base import task


class scheduler:
    _instance = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.queue = queue.Queue()

    def __new__(cls, *args, **kwargs):
        '''基于锁的多线程安全单例'''
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(scheduler, cls).__new__(cls)
        return cls._instance

    def add(self, task: 'task') -> None:
        self.queue.put(task)

    def run(self) -> None:
        temp_num = os.cpu_count()
        if temp_num is None:
            temp_num = CONFIG.THREAD_MAX_NUM_DEFLAUTE
        else:
            temp_num *= 2
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=temp_num) as executor:
            while True:
                task_temp: 'task' = self.queue.get()
                executor.submit(task_temp.run)


SCHEDULER = scheduler()
