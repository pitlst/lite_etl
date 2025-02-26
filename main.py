import signal
import sys
import uvicorn
from utils.config import CONFIG
from utils.connect import CONNECTER
from utils.scheduler import SCHEDULER
from tasks import task_init, task_run

def signal_handler(signum, frame):
    print("正在关闭连接...")
    # 关闭所有数据库连接
    SCHEDULER.stop()
    CONNECTER.close_all()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    # task_run(task_init())
    # uvicorn.run(app=app, host=CONFIG.BACK_HOST, port=CONFIG.BACK_PORT, reload=True)
    task_init()
    