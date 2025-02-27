import signal
import sys
import uvicorn
from utils.scheduler import SCHEDULER
from utils.connect import CONNECT
from tasks import task_init, task_run

def signal_handler(signum, frame):
    print("正在清理数据")
    # 关闭所有数据库连接
    SCHEDULER.stop()
    CONNECT.SQL.close()
    CONNECT.NOSQL.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    task_run(task_init())
    