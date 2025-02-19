from tasks.base import task
from tasks.scheduler import SCHEDULER
from tasks.sync import extract_sql, sync_sql, extract_nosql, load_table
from tasks.incremental import incremental_task, incremental_task_options

def task_init() -> list[task]:
    """
    用于生成任务和定义任务之间的依赖关系
    返回值是需要最开始运行的任务列表
    """
    tasks_group: list[task] = []

    
    return tasks_group

def task_run(input_tasks: list[task]) -> None:
    """运行任务"""
    for task in input_tasks:
        SCHEDULER.add(task)