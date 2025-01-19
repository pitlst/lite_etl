import asyncio
import time
from abc import ABC, abstractmethod
from typing import Callable

from utils import LOGGER_QUEUE

# 所有任务的抽象
class task(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self.next = []
        self.func = None
        
    def then(self, name: str):
        self.next.append(name)
        return self
    
    @abstractmethod
    async def task_main(self):
        ...
    
    async def run(self):
        # 真正运行函数的地方
        start_time = time.time()
        if self.func is None:
            raise ValueError("没有对应的执行函数")
        await self.task_main()
        end_time = time.time()
        spend_time = (end_time - start_time)/60
        LOGGER_QUEUE.put({
            "name":self.name, 
            "type":"debug", 
            "msg":"Spend_time:{} min".format(spend_time)
        })
