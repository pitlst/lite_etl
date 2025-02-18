# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from tasks.web import web_task
from web.example import app

def main():
    print("任务运行测试")
    temp = web_task(app)
    temp.run()

if __name__ == "__main__":
    main()
