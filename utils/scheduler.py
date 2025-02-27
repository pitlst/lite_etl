
import os
import queue
import concurrent.futures

# 仅在类型检查时导入
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from tasks.base import task


class scheduler:
    def __init__(self) -> None:
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 2) # type: ignore
        self._futures: list[concurrent.futures.Future] = []
        
    def stop(self):
        """立即关闭线程池"""
        self._executor.shutdown(wait=False, cancel_futures=True)
        
    def pause(self) -> bool:
        """检查线程池是否运行完成"""
        return all(future.done() for future in self._futures)

    def add(self, task: 'task') -> None:
        """向线程池添加任务"""
        self._futures.append(self._executor.submit(task.run))


SCHEDULER = scheduler()
