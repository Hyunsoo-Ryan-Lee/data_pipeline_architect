from dataclasses import dataclass
import traceback, sys, os
import numpy as np
from datetime import datetime
from loguru import logger
from settings import DB_SETTINGS, JIRA_AUTH, JIRA_ISSUE, LOCAL_LOG_DIR
from utils.connector import DBConnector
from utils.checker import ( DataTypeChecker,
                            TimeDiffChecker,
                            RowCountChecker,
                            ETLChecker )
from utils.utils import DateHandler, TrinoQueryHandler
from utils.logger import MessageProducer, Logging

extract = __import__("pipeline.02_extract", fromlist=[""])
load = __import__("pipeline.03_load", fromlist=[""])
update = __import__("pipeline.04_update", fromlist=[""])
send = __import__("pipeline.05_send", fromlist=[""])

@dataclass
class Data_Pipeline:
    parallelism_info: dict
    execution_info: dict
    extract_info: dict
    load_info: dict
    update_info: dict
    logging_info: dict

    def __post_init__(self):
        # parallelism_info VALUES
        self.PAR_index: int = int(self.parallelism_info["index"])
        self.PAR_parallel: int = int(self.parallelism_info["parallel"])
        
        # execution_info VALUES
        self.EXEC_execution_job: str = self.execution_info["execution_job"]
        self.EXEC_execution_interval: str = self.execution_info["execution_interval"]
        self.EXEC_execution_data: str = self.execution_info["execution_data"]
        self.EXEC_execution_database: str = self.execution_info["execution_database"]
        self.EXEC_execution_name: str = self.execution_info["execution_name"]
        self.EXEC_partition_date: str = self.execution_info["partition_date"]

        # extract_info VALUES
        self.EXT_table_query_dict: dict = self.extract_info["module_location"].queries
        self.EXT_database_name: str = self.extract_info["database_name"]
        self.EXT_query_file: str = self.extract_info["query_file"]
        self.EXT_query_location: str = self.extract_info["query_location"].replace("-", "_")

        # load_info VALUES
        self.LOAD_storage_location: str = self.load_info["storage_location"]
        
        # update_info VALUES
        self.UPD_trino_connection =  DBConnector(**DB_SETTINGS['trino_dev'])
        self.UPD_catalog_name: str = self.update_info["catalog_name"]
        self.UPD_schema_name: str = self.update_info["schema_name"]
        
        # logging_info VALUES
        self.LOG_kafka_topic: str = self.logging_info["kafka_topic"]
        self.LOG_kafka_broker: list[str] = self.logging_info["kafka_broker"]
        self.LOG_log_location:str = os.path.join(LOCAL_LOG_DIR, self.EXEC_partition_date[:4], self.EXEC_partition_date[4:6], self.EXEC_partition_date[6:8], self.EXEC_execution_job)
        
        # imported Classes
        self.DataTypeChecker = DataTypeChecker()
        self.RowCountChecker = RowCountChecker()
        self.TrinoQueryHandler = TrinoQueryHandler()
        self.ETLChecker = ETLChecker()
        self.MessageProducer = MessageProducer(self.LOG_kafka_broker, self.LOG_kafka_topic)
    
    def PIPELINE_INIT(self):
        _id = Logging.get_function_name()
        logger.debug(f"{_id} START")
        self.SPLIT_TABLE_LIST = np.array_split(list(self.EXT_table_query_dict), self.PAR_parallel)

        if len(self.SPLIT_TABLE_LIST[self.PAR_index]) == 0:
            logger.warning("NO JOB TO EXECUTE. JOB FINISH.")
            sys.exit(0)
        logger.debug(f"{_id} END")
        return self
    
    def PIPELINE_START(self, _table_name):
        _id = Logging.get_function_name()
        logger.debug(f"{_id} START")
        self.start_table_timestamp = DateHandler.get_timestamp()
        self._connection = DBConnector(**DB_SETTINGS[self.EXT_query_location])
        self._query = self.EXT_table_query_dict.get(_table_name).format(schema_name=self.EXT_database_name, partition_date = self.EXEC_partition_date)
        self.is_exist = self.DataTypeChecker.data_exist_checker(self._connection, self._query)
        logger.debug(f"{_id} END")
        
        return self
    
    def DATA_EXTRACT(self, _table_name):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        logger.debug(f"{_id} START")
        self._extract_data, self.data_type_checker, self.row_count_checker = \
            extract.table_data_extractor(
                db_connector = self._connection,
                data_type_checker = self.DataTypeChecker,
                row_count_checker = self.RowCountChecker,
                schema_name = self.EXT_database_name,
                table_name = _table_name,
                query = self._query
            )
        logger.debug(f"{_id} END")
        return self
        
    def DATA_UPLOAD(self, _table_name):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        logger.debug(f"{_id} START")
        self._parent_path, self._save_path, self.data_type_checker, self.row_count_checker = \
            load.partition_uploader(
                pdf_data = self._extract_data,
                data_type_checker = self.data_type_checker,
                row_count_checker = self.row_count_checker,
                s3_storage_path = self.LOAD_storage_location,
                data_name = self.EXEC_execution_data,
                location_prefix_name = self.EXEC_execution_database,
                schema_name = self.EXT_database_name,
                table_name = _table_name,
                partition_date = self.EXEC_partition_date,
            )
        logger.debug(f"{_id} END")
        return self
            
    def DATA_UPDATE(self, _table_name):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        logger.debug(f"{_id} START")
        update.metadata_updater(
                db_connector = self.UPD_trino_connection.conn,
                table_query_handler = self.TrinoQueryHandler,
                pdf_data = self._extract_data,
                parent_path = self._parent_path,
                execution_interval = self.EXEC_execution_interval,
                catalog_name = self.UPD_catalog_name.lower().replace('-', '_'),
                schema_name = self.UPD_schema_name.lower().replace('-', '_'),
                table_name = _table_name.lower(),
        )
        logger.debug(f"{_id} END")
    
    def DATA_INFORM(self, _table_name):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        logger.debug(f"{_id} START")
        self.end_table_timestamp = DateHandler.get_timestamp()
        self.ETLChecker.set_total_time_diff_info(TimeDiffChecker(self.start_table_timestamp, self.end_table_timestamp).get_execution_time_diff_info(self.EXT_database_name, _table_name))
        self.ETLChecker.set_total_dtype_info(self.data_type_checker.get_dtype_info(self.EXT_database_name, _table_name))
        self.ETLChecker.set_total_row_count_info(self.row_count_checker.get_row_count_info(self.EXT_database_name, _table_name))
        self.ETLChecker.join_total_count_duration_info()
        logger.debug(f"{_id} END")
        return self
    
    def DATA_LOGGING_S3(self):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        logger.debug(f"{_id} START")
        load.log_uploader(
            pdf_data = self.ETLChecker.total_table_info,
            s3_storage_path = self.LOAD_storage_location,
            data_name = self.EXEC_execution_data,
            location_prefix_name = self.EXEC_execution_database,
            schema_name = self.EXT_database_name,
            partition_date = self.EXEC_partition_date,
            file_name = self.EXEC_execution_name
        )
        logger.debug(f"{_id} END")
        
    def DATA_LOGGING_ES(self):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        logger.debug(f"{_id} START")
        try:
            send.log_sender(table_list = [self.ETLChecker.total_table_info],
                            message_producer =  self.MessageProducer)
        except:
            logger.exception("EXCEPTION OCCURRED")
            err = f"<<<{datetime.now():%Y%m%d %H:%M:%S} - ERROR LIST>>>\n\n" 
            err += traceback.format_exc()
            JIRA_AUTH.issue_add_comment(JIRA_ISSUE, err)
            logger.exception(f"ISSUE ADDED TO JIRA {JIRA_ISSUE}")
        logger.debug(f"{_id} END")

    def PIPELINE_END(self):
        _id = Logging.get_function_name()
        print("="*20,_id,"="*20)
        log_list = [self.ETLChecker.total_table_info, self.ETLChecker.total_dtype_info]
        Logging.excel_writer(self.LOG_log_location, self.EXEC_execution_job, log_list)
        logger.debug(f"{_id} END")