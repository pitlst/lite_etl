import uvicorn
from utils.config import CONFIG
from web.back.app import app
from tasks import task_init, task_run

if __name__ == "__main__":
    task_run(task_init())
    uvicorn.run(app=app, host=CONFIG.BACK_HOST, port=CONFIG.BACK_PORT, reload=True)