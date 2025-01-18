import time
import multiprocessing as mp
from typing import Callable

from utils import LOGGER_QUEUE

# 所有任务的抽象
class task:
    def __init__(self, name: str) -> None:
        self.name = name
        self.next = []
        self.func = None
        
    def then(self, name: str):
        self.next.append(name)
        return self
    
    def set(self, func: Callable):
        self.func = func
        return self
    
    def run(self):
        # 真正运行函数的地方
        start_time = time.time()
        if not self.func is None:
            self.func()
        end_time = time.time()
        spend_time = (end_time - start_time)/60
        LOGGER_QUEUE.put({
            "name":self.name, 
            "type":"debug", 
            "msg":"Spend_time:{} min".format(spend_time)
        })
