import dash
import flask
from tasks.base import task
from utils.config import CONFIG

main_server = flask.Flask()


class web_task(task):
    '''用于运行web前端的任务，使其兼容目前的调度器，该任务死循环不会退出'''

    def __init__(self, web_app: dash.Dash) -> None:
        super().__init__(web_app.server.name)
        self.app = web_app

    def task_main(self) -> None:
        self.app.run(host=CONFIG.BI_HOST, port=str(CONFIG.BI_PORT), debug=CONFIG.DEBUG)
