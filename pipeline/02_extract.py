import pandas as pd
import sqlalchemy
from loguru import logger
from utils.utils import QueryHandler
from utils.logger import Logging


def table_data_extractor(db_connector, data_type_checker, row_count_checker, schema_name: str, table_name: str, query: str):
    _id = Logging.get_function_name()
    logger.info(_id)
    with db_connector as connected:
        
        source_dtype = globals()[f"_get_{connected.conn_engine}_dtype"]( connection = connected,
                                                                    schema_name = schema_name,
                                                                    table_name = table_name,
                                                                    query = query               )
        data_type_checker.set_source_dtype(source_dtype)


        extract_data = _get_extract_data_from_sql(  connection = connected,
                                                    datatype = data_type_checker,
                                                    query = query                   )

        data_type_checker.set_extract_dtype(data_type_checker.get_dtype_from_pdf(extract_data, 'EXTRACT_DTYPE'))
        row_count_checker.set_extract_row_count(extract_data)
    logger.success(f"{_id} SUCCESS")
    return extract_data, data_type_checker, row_count_checker

def _get_MYSQL_dtype(connection, schema_name: str, table_name: str, query: str):
    _id = Logging.get_function_name()
    logger.info(_id)
    
    # 해당 테이블의 INFORMATION SCHEMA 쿼리
    mysql_dtype_extract_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
    """
    try:
        mysql_dtype = pd.read_sql_query(mysql_dtype_extract_query, connection.conn)
        logger.success(f"{_id} SUCCESS")
    except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.OperationalError) as e:
        mysql_dtype = None
        logger.exception(e)
    return mysql_dtype


def _get_extract_data_from_sql(connection, datatype, query: str):
    _id = Logging.get_function_name()
    logger.info(_id)
    try:
        pandas_df = pd.read_sql_query(query, connection.conn, dtype=dict(datatype._conversion.values))
        pandas_df[datatype.bool.COLUMN_NAME] = pandas_df[datatype.bool.COLUMN_NAME].astype(pd.Int64Dtype())
        pandas_df[datatype.int.COLUMN_NAME] = pandas_df[datatype.int.COLUMN_NAME].astype(pd.Int64Dtype())
        for col in pandas_df.select_dtypes('object').columns:
            pandas_df[col] = pandas_df[col].astype(str)
        logger.success(f"{_id} SUCCESS")
    except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.OperationalError) as e:
        pandas_df = pd.read_sql_query(query, connection.conn)
        for col in pandas_df.select_dtypes('object').columns:
            pandas_df[col] = pandas_df[col].astype(str)
        logger.exception(e)
    return pandas_df