# 连接数据库测试
from utils import EXECUTER
# 任务运行测试
from tasks import task
class temp_task(task):
    def __init__(self, name):
        super().__init__(name)
        
    def task_main(self):
        print("测试")
        return super().task_main()
    
temp = temp_task("测试")
temp.run()