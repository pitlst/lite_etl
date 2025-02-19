
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
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 2) # type: ignore
        self._futures: list[concurrent.futures.Future] = []
        def main() -> None:            
            while True:
                task_temp: 'task' = self._queue.get()
                self._futures.append(self._executor.submit(task_temp.run))
        self._executor.submit(main)
        
    def __del__(self):
        self._executor.shutdown()
        
    def pause(self) -> bool:
        return all(future.done() for future in self._futures) and self._queue.empty()

    def add(self, task: 'task') -> None:
        self._queue.put(task)


SCHEDULER = scheduler()
