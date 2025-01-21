import time
import traceback
from abc import ABC, abstractmethod
from utils import make_logger


# 所有任务的抽象
class task(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self.log = make_logger(self.name)
        self.next = []
        self.func = None
        
    def then(self, name: str):
        self.next.append(name)
        return self
    
    # 继承后实现逻辑的地方
    @abstractmethod
    def task_main(self):
        ...
    
    def run(self):
        # 真正运行函数的地方
        start_time = time.time()
        if self.func is None:
            raise ValueError("没有对应的执行函数")
        try:
            self.task_main()
        except Exception as e:
            self.log.critical("报错内容：" + str(e))
            self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
        end_time = time.time()
        self.log.debug("函数花费时间为:{} 秒".format(end_time - start_time))