import sys
from settings import BASE_DIR
from pipeline.controller import Pipeline_Modules
from utils.utils import DateHandler, QuerySelector

def start_batch(EXECUTION_JOB, PARALLELISM, COUNT_INDEX):
    
    job_config = QuerySelector(BASE_DIR, "jobs", EXECUTION_JOB).get_query_module().task_info
    job_config.execution_info.execution_location = QuerySelector(BASE_DIR, "jobs", EXECUTION_JOB).get_query_module()
    job_config.parallelism_info.parallel = PARALLELISM
    job_config.parallelism_info.index = COUNT_INDEX
    job_config.execution_info.execution_job = EXECUTION_JOB
    execution_name = job_config.execution_info.execution_name
    execution_function_group = job_config.execution_info.execution_group
    
    execution_type = job_config.execution_info.execution_type
    execution_interval = job_config.execution_info.execution_interval
    partition_delta = job_config.execution_info.partition_delta
    query_location = job_config.extract_info.query_location
    query_file = job_config.extract_info.query_file

    if execution_type == 'schedule':
        job_config.execution_info.partition_date = DateHandler.get_execution_date(execution_interval, partition_delta)
        job_config.extract_info.module_location = QuerySelector(BASE_DIR, query_location, query_file).get_query_module()
    
    try:
        getattr(Pipeline_Modules(**job_config), execution_function_group[execution_name])()
    except Exception as e:
        print(e)
        sys.exit(1)
    
if __name__ == "__main__":
    start_batch()