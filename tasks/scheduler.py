
import os
import queue
import concurrent.futures
from utils.config import CONFIG

# 仅在类型检查时导入
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from tasks.base import task


class scheduler:
    def __init__(self) -> None:
        self.queue = queue.Queue()
        temp_num = os.cpu_count() if os.cpu_count() is None else 5
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=temp_num)
        def main() -> None:            
            while True:
                task_temp: 'task' = self.queue.get()
                self.executor.submit(task_temp.run)
        self.executor.submit(main)
        
    def __del__(self):
        self.executor.shutdown()

    def add(self, task: 'task') -> None:
        self.queue.put(task)


SCHEDULER = scheduler()
