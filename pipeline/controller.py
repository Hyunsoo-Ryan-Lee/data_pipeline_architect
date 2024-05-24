from dataclasses import dataclass
from loguru import logger
from utils.logger import Logging
import os

Data_Pipeline = __import__("pipeline.01_skeleton", fromlist=[""]).Data_Pipeline

@dataclass
class Pipeline_Modules(Data_Pipeline):
    def __post_init__(self):
        super().__post_init__()
        logger.add(os.path.join(f"{self.LOG_log_location}", f"{self.EXEC_execution_job}_{self.PAR_index}.log"))
    
    def TEMP_DEV(self):
        _id = Logging.get_function_name()
        logger.debug(f"{_id} START")
        self.PIPELINE_INIT()
        for idx, TABLE_NAME in enumerate(self.SPLIT_TABLE_LIST[self.PAR_index]):
            self.PIPELINE_START(TABLE_NAME)
            # DB에 TABLE_NAME 이름의 테이블이 있는지 확인
            if self.is_exist:
                logger.info(f"{TABLE_NAME}----------{idx+1}")
                self.DATA_EXTRACT(TABLE_NAME)
                # TABLE_NAME 테이블의 데이터가 있는지 확인
                if len(self._extract_data) > 0:
                    self.DATA_UPLOAD(TABLE_NAME)
                    self.DATA_UPDATE(TABLE_NAME)
                    self.DATA_INFORM(TABLE_NAME)
                else:
                    logger.warning(f"{TABLE_NAME} ROW COUNT IS ZERO")
        """
        이행 대상 테이블들 중 데이터가 하나라도 있는 것이 있으면
        total_table_info 객체가 생성되므로 이행 관련 정보를 로깅한다.
        """
        if hasattr(self.ETLChecker, "total_table_info"):
            self.DATA_LOGGING_S3()
            self.DATA_LOGGING_ES()
            self.PIPELINE_END()
        else:
            logger.warning(f"NO DATA IN {self.EXEC_execution_database}.{self.EXT_database_name} DATABASE")
        logger.debug(f"{_id} END")


    def TEMP_PRD(self):
        _id = Logging.get_function_name()
        logger.debug(f"{_id} START")
        self.PIPELINE_INIT()
        for idx, TABLE_NAME in enumerate(self.SPLIT_TABLE_LIST[self.PAR_index]):
            self.PIPELINE_START(TABLE_NAME)
            # DB에 TABLE_NAME 이름의 테이블이 있는지 확인
            if self.is_exist:
                logger.info(f"{TABLE_NAME}----------{idx+1}")
                self.DATA_EXTRACT(TABLE_NAME)
                # TABLE_NAME 테이블의 데이터가 있는지 확인
                if len(self._extract_data) > 0:
                    self.DATA_UPLOAD(TABLE_NAME)
                    self.DATA_UPDATE(TABLE_NAME)
                    self.DATA_INFORM(TABLE_NAME)
                else:
                    logger.warning(f"{TABLE_NAME} ROW COUNT IS ZERO")
        """
        이행 대상 테이블들 중 데이터가 하나라도 있는 것이 있으면
        total_table_info 객체가 생성되므로 이행 관련 정보를 로깅한다.
        """
        if hasattr(self.ETLChecker, "total_table_info"):
            self.DATA_LOGGING_S3()
            self.DATA_LOGGING_ES()
            self.PIPELINE_END()
        else:
            logger.warning(f"NO DATA IN {self.EXEC_execution_database}.{self.EXT_database_name} DATABASE")
        logger.debug(f"{_id} END")
