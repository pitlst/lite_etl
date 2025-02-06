import time
import sqlalchemy
import traceback
import logging
from abc import ABC, abstractmethod
from utils import make_logger, CONFIG
    
class task_connect_with:
    '''用来处理连接异常的上下文管理器'''
    def __init__(self, engine: sqlalchemy.engine.Engine, log: logging.Logger):
        self.engine = engine
        self.connection = engine.connect()
        self.log = log

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback_info):
        # 退出上下文时，处理异常并回滚事务
        if exc_type is not None:  # 如果有异常发生
            self.log.critical("报错类型：" + str(exc_type))
            self.log.critical("报错内容：" + str(exc_value))
            self.log.critical("报错堆栈信息：" + str(traceback_info.format_exc()))
            # 回退操作
            self.connection.rollback()
        else:
            # 如果没有异常，提交事务
            self.connection.commit()  
        return True

class task(ABC):
    '''所有任务的抽象'''
    def __init__(self, name: str) -> None:
        self.name = name
        self.log = make_logger(self.name)
        self.next = []
        
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
        try:
            self.task_main()
        except Exception as e:
            self.log.critical("报错内容：" + str(e))
            self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
        end_time = time.time()
        self.log.debug("函数花费时间为:{} 秒".format(end_time - start_time))
        
        

    
