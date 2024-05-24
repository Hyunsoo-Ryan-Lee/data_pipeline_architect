from loguru import logger
from utils.logger import Logging
from trino.exceptions import OperationalError

def metadata_updater( db_connector,
                      table_query_handler,
                      pdf_data,
                      parent_path,
                      execution_interval,
                      catalog_name,
                      schema_name,
                      table_name ):
    
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    exist_schema = schema_checker(db_connector, table_query_handler, catalog_name, schema_name)
    # catalog에 scheam 부재 -> schema create
    if not exist_schema:
        schema_creator(db_connector, table_query_handler, catalog_name, schema_name)
    
    # catalog.schema에 table 존재
    exist_table = table_checker(db_connector, table_query_handler, catalog_name, schema_name, table_name)
    # catalog.schema에 table 부재 -> table create
    if not exist_table:
        table_creator(db_connector,
                    table_query_handler,
                    pdf_data,
                    parent_path,
                    execution_interval,
                    catalog_name,
                    schema_name,
                    table_name )

    # catalog.schema에 table 존재 -> table update
    table_updater(db_connector, table_query_handler, catalog_name, schema_name, table_name)
    logger.success(f"{_id} SUCCESS")
        
def schema_checker(db_connector, table_query_handler, catalog_name, schema_name):
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    _schema_check_query = table_query_handler.schema_check_query(catalog_name, schema_name)
    with db_connector as connected:
        try:
            cur = connected.cursor()
            cur.execute(_schema_check_query)
            res = True if cur.fetchone() else False
            logger.success(f"{_id} SUCCESS")
        except OperationalError as e:
            logger.exception(e)
    return res
    
def table_checker(db_connector, table_query_handler, catalog_name, schema_name, table_name):
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    _table_check_query = table_query_handler.table_check_query(catalog_name, schema_name, table_name)
    with db_connector as connected:
        try:
            cur = connected.cursor()
            cur.execute(_table_check_query)
            res = True if cur.fetchone() else False
            logger.success(f"{_id} SUCCESS")
        except OperationalError as e:
            logger.exception(e)
    return res
    
def schema_creator(db_connector, table_query_handler, catalog_name, schema_name):
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    _schema_create_query = table_query_handler.schema_create_query(catalog_name, schema_name)
    with db_connector as connected:
        try:
            cur = connected.cursor()
            cur.execute(_schema_create_query)
            logger.success(f"{_id} SUCCESS")
        except OperationalError as e:
            logger.exception(e)

def table_creator( db_connector,
                   table_query_handler,
                   pdf_data,
                   parent_path,
                   execution_interval,
                   catalog_name,
                   schema_name,
                   table_name ):
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    _table_create_query = \
        table_query_handler.table_create_query(
        pdf_data,
        parent_path,
        execution_interval,
        catalog_name,
        schema_name,
        table_name
    )
    with db_connector as connected:
        try:
            cur = connected.cursor()
            cur.execute(_table_create_query)
            logger.success(f"{_id} SUCCESS")
        except OperationalError as e:
            logger.exception(e)
    
def table_updater(db_connector, table_query_handler, catalog_name, schema_name, table_name):
    _id = Logging.get_function_name()
    logger.info(f"{_id}")
    _schema_create_query = table_query_handler.table_update_query(catalog_name, schema_name, table_name)
    with db_connector as connected:
        try:
            cur = connected.cursor()
            cur.execute(_schema_create_query)
            logger.success(f"{_id} SUCCESS")
        except OperationalError as e:
            logger.exception(e)