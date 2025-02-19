
import os
import queue
import concurrent.futures

# 仅在类型检查时导入
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from tasks.base import task


class scheduler:
    def __init__(self) -> None:
        self._queue = queue.Queue()
        temp_num = os.cpu_count() if os.cpu_count() is None else 5
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=temp_num)
        def main() -> None:            
            while True:
                task_temp: 'task' = self._queue.get()
                self._executor.submit(task_temp.run)
        self._executor.submit(main)
        
    def __del__(self):
        self._executor.shutdown()

    def add(self, task: 'task') -> None:
        self._queue.put(task)


SCHEDULER = scheduler()
