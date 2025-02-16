# import test_connect
# import test_task
# import test_sync_total

# if __name__ == "__main__":
    # print("开始全部测试")
    # test_connect.main()
    # print("-------------------成功-------------------")
    # test_task.main()
    # print("-------------------成功-------------------")
    # test_sync_total.main_sync_sql()
    # print("-------------------成功-------------------")
    # test_sync_total.main_extract_sql()
    # print("-------------------成功-------------------")
    # test_sync_total.main_extract_nosql()
    # print("-------------------成功-------------------")
    # test_sync_total.main_load_table()
    # print("-------------------成功-------------------")
    # print("-------------------全部测试完成-------------------")
    
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from re import L

@dataclass
class incremental_task_options:
    '''增量同步的函数初始化参数类'''
    name: str
    sync_sql_path: str
    sync_source_connect_name: str
    local_table_name: str
    # 用于指定查询模板中哪些列是用于比较数据是否变更的，默认指定第一列为id
    incremental_comparison_list: list[int] = field(default_factory=lambda: [0])
    # 用于指定相关分录的查询模板
    other_entry_sql_path: list[str] = field(default_factory=list)
    local_schema: str = "ods"
    temp_table_schema: str = "m_temp"
    chunksize: int = 10000
    # 对于查询中没有的数据是否删除
    is_delete: bool = False



class Base(ABC):
    def __init__(self) -> None:
        self.is_disabled = False
    
    @abstractmethod
    def virtual_method(self):
        ...

    def call_virtual(self):
        try:
            self.virtual_method()
        except Exception as e:
            print(e)
            # self.log.critical("报错内容：" + str(e))
            # self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
        # self.virtual_method()
        
class Derived1(Base):
    def __init__(self, option: incremental_task_options) -> None:
        self.option = option
        super().__init__()
    
    def virtual_method(self):
        print("Derived1's implementation of virtual_method")
        
class Derived2(Base):
    def __init__(self, option: incremental_task_options) -> None:
        self.option = option
        super().__init__()
    
    def virtual_method(self):
        print("Derived2's implementation of virtual_method")
        
def main():
    # 实例化并调用
    derived1 = Derived1(incremental_task_options(name="增量同步测试",
                                                sync_sql_path="test/test_sync_incremental.sql",
                                                sync_source_connect_name="mysql测试",
                                                local_table_name="test_sync_incremental",
                                                incremental_comparison_list=[0, 4],
                                                is_delete=True
                                                ))
    derived1_ = Derived1(incremental_task_options(name="增量同步测试",
                                                sync_sql_path="test/test_sync_incremental.sql",
                                                sync_source_connect_name="mysql测试",
                                                local_table_name="test_sync_incremental",
                                                incremental_comparison_list=[0, 4],
                                                is_delete=True
                                                ))
    derived1.call_virtual()  # 输出结果
    
if __name__ == "__main__":
    main()