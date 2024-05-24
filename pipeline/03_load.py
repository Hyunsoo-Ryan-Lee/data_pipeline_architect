import os
from loguru import logger
from utils.logger import Logging
import pandas as pd
import settings

def partition_uploader(  pdf_data: pd.DataFrame,
                            data_type_checker,
                            row_count_checker,
                            s3_storage_path: str,
                            data_name: str,
                            location_prefix_name: str,
                            schema_name: str,
                            table_name: str,
                            partition_date: str ):

    _id = Logging.get_function_name()
    logger.info(_id)  
    
    parent_path, data_path = \
        _get_data_path( s3_storage_path = s3_storage_path,
                        data_name = data_name,
                        location_prefix_name = location_prefix_name,
                        schema_name = schema_name,
                        table_name = table_name,
                        partition_date = partition_date )
        
    save_path = _upload_pandas_to_parquet(  pandas_df = pdf_data,
                                          s3_save_path = data_path,
                                          table_name = table_name )

    # DataTypeChecker의 self.transform_dtype에 컬럼 타입 변환된 df 저장
    data_type_checker.set_transform_dtype(data_type_checker.get_dtype_from_pdf(pdf_data, 'TRANSFORM_DTYPE'))
    
    # DataTypeChecker의 self.set_transform_row_count에 타입 변환 df row 수 저장 
    row_count_checker.set_transform_row_count(pdf_data)
    logger.success(f"{_id} SUCCESS")
    return parent_path, save_path, data_type_checker, row_count_checker

def log_uploader(pdf_data: pd.DataFrame,
                 s3_storage_path: str,
                 data_name: str,
                 location_prefix_name: str,
                 schema_name: str,
                 partition_date: str,
                 file_name: str ):
    _id = Logging.get_function_name()
    logger.info(_id)
    
    _log_path = _get_log_path( s3_storage_path = s3_storage_path,
                               data_name = data_name,
                               location_prefix_name = location_prefix_name,
                               schema_name = schema_name,
                               partition_date = partition_date )

    save_path = _upload_pandas_to_parquet(  pandas_df = pdf_data,
                                          s3_save_path = _log_path,
                                          table_name = file_name )
    
    logger.success(f"{_id} SUCCESS")
    
def _get_data_path( s3_storage_path: str,
                    data_name: str,
                    location_prefix_name: str,
                    schema_name: str,
                    table_name: str,
                    partition_date: str  ):
    _id = Logging.get_function_name()
    logger.info(_id)
    if len(partition_date) == 8:
        partition_path = f"yyyy={partition_date[0:4]}/mm={partition_date[4:6]}/dd={partition_date[6:8]}"
    elif len(partition_date) == 6:
        partition_path = f"yyyy={partition_date[0:4]}/mm={partition_date[4:6]}"
    elif len(partition_date) == 4:
        partition_path = f"yyyy={partition_date[0:4]}"
    else:
        raise RuntimeError(f"The partition_date format is not allowed.")
    
    parent_path = os.path.join(s3_storage_path, data_name, location_prefix_name, schema_name, table_name).replace("\\", "/")
    data_path = os.path.join(parent_path, partition_path).replace("\\", "/")
    logger.success(f"{_id} SUCCESS")
    return parent_path, data_path


def _get_log_path( s3_storage_path: str,
                   data_name: str,
                   location_prefix_name: str,
                   schema_name: str,
                   partition_date: str  ):
    _id = Logging.get_function_name()
    logger.info(_id)
    if len(partition_date) == 8:
        partition_path = f"yyyy={partition_date[0:4]}/mm={partition_date[4:6]}/dd={partition_date[6:8]}"
    elif len(partition_date) == 6:
        partition_path = f"yyyy={partition_date[0:4]}/mm={partition_date[4:6]}"
    elif len(partition_date) == 4:
        partition_path = f"yyyy={partition_date[0:4]}"
    else:
        raise RuntimeError(f"The partition_date format is not allowed.")
    
    parent_path = os.path.join(s3_storage_path, f"{data_name}_log", location_prefix_name, schema_name).replace("\\", "/")
    data_path = os.path.join(parent_path, partition_path).replace("\\", "/")
    logger.success(f"{_id} SUCCESS")
    return data_path


def _upload_pandas_to_parquet(pandas_df, s3_save_path: str, table_name: str):
    _id = Logging.get_function_name()
    logger.info(_id)
    try:
        save_name = table_name+'.parquet'
        save_path = os.path.join(s3_save_path, save_name).replace("\\", "/")
        storage_options = settings.NCP_AUTH
        pandas_df.to_parquet( save_path,
                                engine='pyarrow',
                                compression='gzip',
                                storage_options = storage_options,
                                index=False )
        logger.success(f"{_id} SUCCESS")
    except Exception as e:
        save_path = None
        logger.exception("EXCEPTION OCCURRED")
        print(e)

    return save_path