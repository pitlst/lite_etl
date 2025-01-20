import time
from abc import ABC, abstractmethod
from utils import EXECUTER, make_logger


# 所有任务的抽象
class task(ABC):
    async def __init__(self, name: str) -> None:
        self.name = name
        self.log = await make_logger(self.name)
        self.next = []
        self.func = None
        
    async def then(self, name: str):
        self.next.append(name)
        return self
    
    # 继承后实现逻辑的地方
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
        self.log.debug("函数花费时间为:{} 秒".format(end_time - start_time))