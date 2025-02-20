import time
from utils.config import CONFIG
from tasks.base import task
from utils.scheduler import SCHEDULER
from tasks.sync import extract_sql, sync_sql, extract_nosql, load_table
from tasks.incremental import incremental_task, incremental_task_options

def task_init() -> list[task]:
    """
    用于生成任务和定义任务之间的依赖关系
    返回值是需要最开始运行的任务列表
    """
    tasks_group: list[task] = []
    
    # # 改善同步
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "改善数据同步",
    #         sync_sql_path = "ameliorate/ameliorate.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "ameliorate",
    #         incremental_comparison_list = [0, 2], 
    #         is_delete=True  
    #     )
    # ))
    # # 短信同步
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "短信数据同步",
    #         sync_sql_path = "short_message/short_message.sql",
    #         sync_source_connect_name = "EAS",
    #         local_table_name = "short_message",
    #         incremental_comparison_list = [0, 2], 
    #         is_delete=False
    #     )
    # ))
    # 唐渝用-调试项点同步
    tasks_group.append(incremental_task(
        incremental_task_options(
            name = "唐渝用-调试项点同步",
            sync_sql_path = "tangyu/alignment_file.sql",
            sync_source_connect_name = "MES",
            local_table_name = "alignment_file",
            incremental_comparison_list = [0, 1], 
            is_delete=False
        )   
    ))
    # 业联系统任务同步
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-业联执行关闭同步",
    #         sync_sql_path = "business_connection/business_connection_close.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "business_connection_close",
    #         incremental_comparison_list = [0, 25], 
    #         other_entry_sql_path = {
    #             "business_connection_copy_delivery_unit":"business_connection/business_connection_copy_delivery_unit.sql",
    #             "business_connection_main_delivery_unit":"business_connection/business_connection_main_delivery_unit.sql"
    #         },
    #         is_delete=False
    #     )
    # ))
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-车间联络单同步",
    #         sync_sql_path = "business_connection/business_connection.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "business_connection",
    #         incremental_comparison_list = [0, 21, 24, 27], 
    #         is_delete=False
    #     )
    # ))
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-班组同步",
    #         sync_sql_path = "business_connection/class_group.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "class_group",
    #         incremental_comparison_list = [0, 15], 
    #         other_entry_sql_path={
    #             "class_group_entry":"business_connection/class_group_entry.sql"
    #         },
    #         is_delete=False
    #     )
    # ))
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-设计变更同步",
    #         sync_sql_path = "business_connection/design_change.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "design_change",
    #         incremental_comparison_list = [0, 4, 10], 
    #         other_entry_sql_path={
    #             "design_change_entry":"business_connection/design_change_entry.sql"
    #         },
    #         is_delete=False
    #     )
    # ))
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-设计变更执行同步",
    #         sync_sql_path = "business_connection/design_change_execution.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "design_change_execution",
    #         incremental_comparison_list = [0, 4, 10], 
    #         other_entry_sql_path={
    #             "design_change_execution_reworked_material":"business_connection/design_change_execution_reworked_material.sql",
    #             "design_change_execution_reworked_material_unit":"business_connection/design_change_execution_reworked_material_unit.sql",
    #             "design_change_execution_material_preparation_technology":"business_connection/design_change_execution_material_preparation_technology.sql",
    #             "design_change_execution_material_preparation_technology_unit":"business_connection/design_change_execution_material_preparation_technology_unit.sql",
    #             "design_change_execution_material_change":"business_connection/design_change_execution_material_change.sql",
    #             "design_change_execution_main_delivery_unit":"business_connection/design_change_execution_main_delivery_unit.sql",
    #             "design_change_execution_handle":"business_connection/design_change_execution_handle.sql",
    #             "design_change_execution_document_change":"business_connection/design_change_execution_document_change.sql",
    #             "design_change_execution_document_change_unit":"business_connection/design_change_execution_document_change_unit.sql",
    #             "design_change_execution_copy_delivery_unit":"business_connection/design_change_execution_copy_delivery_unit.sql",
    #             "design_change_execution_change_content":"business_connection/design_change_execution_change_content.sql",
    #             "design_change_execution_audit":"business_connection/design_change_execution_audit.sql"
    #         },
    #         is_delete=False
    #     )
    # ))
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-车间执行单同步",
    #         sync_sql_path = "business_connection/design_change_execution.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "design_change_execution",
    #         incremental_comparison_list = [0, 4, 10], 
    #         other_entry_sql_path={
    #         },
    #         is_delete=False
    #     )
    # ))
    # tasks_group.append(incremental_task(
    #     incremental_task_options(
    #         name = "业联-工艺流程同步",
    #         sync_sql_path = "business_connection/technological_process.sql",
    #         sync_source_connect_name = "金蝶云苍穹-正式库",
    #         local_table_name = "technological_process",
    #         incremental_comparison_list = [0, 4, 10], 
    #         other_entry_sql_path={
    #             "technological_process_flow":"business_connection/technological_process_flow.sql",
    #             "technological_process_change":"business_connection/technological_process_change.sql"
    #         },
    #         is_delete=False
    #     )
    # ))
    return tasks_group

def task_run(input_tasks: list[task]) -> None:
    """运行任务"""
    # 首次运行不等待间隔
    for task in input_tasks:
        SCHEDULER.add(task)
    while True:
        # 只有所有任务同步完成再进行
        if SCHEDULER.pause():
            print("本轮任务运行完成，开始等待")
            time.sleep(CONFIG.INTERVAL_DURATION)
            for task in input_tasks:
                SCHEDULER.add(task)
