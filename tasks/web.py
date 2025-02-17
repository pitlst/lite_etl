from tasks.base import task
from utils.config import CONFIG
from web import app


class web_task(task):
    '''用于运行web前端的任务，使其兼容目前的调度器，该任务死循环不会退出'''

    def __init__(self) -> None:
        super().__init__("dashweb看板前端")

    def task_main(self) -> None:
        app.run_server(host=CONFIG.BI_HOST, port=CONFIG.BI_PORT, debug=CONFIG.DEBUG)
