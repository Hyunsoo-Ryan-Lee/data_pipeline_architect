
import settings
from easydict import EasyDict

task_info = EasyDict(
    dict(
        # 병렬 처리 개수 (외부에서 유입)
        parallelism_info = dict(),
        # 작업의 전체적인 정보
        execution_info = dict(
            execution_name = "D_SOURCEtoBRONZE_temp_prd", # [수정] 작업의 이름
            execution_interval = "day", # 작업 배치 빈도
            execution_type = "schedule",
            partition_delta = -1, # 오늘 기준 몇 일 전의 데이터 이행
            execution_data = "temp",
            execution_database = "database_a", # 이행 대상 데이터베이스 인스턴스 이름
            execution_group = settings.FUNCTION_GROUP
        ),
        # 추출할 테이블 관련 내용
        extract_info = dict(
            query_location = "prd", # [수정] db/temp 폴더 내 최상위 디렉토리 이름
            query_file = "database_prd_a", # [수정] 이행 쿼리가 저장된 파일 이름
            database_name = "database_prd_a" # [수정] 이행 쿼리의 데이터베이스
        ),
        # 이행될 STORAGE 관련 내용
        load_info = dict(
            storage_location = settings.S3_STORAGE, # 데이터가 저장될 버킷 경로
            ),
        # TRINO DB 관련 내용
        update_info = dict(
            catalog_name = "temp", # [수정] Trino 카탈로그 이름
            schema_name = "database_a", # [수정] Trino 스키마 이름 (DB 레벨)
            ),
        # 작업 마무리시 로깅에 관한 내용
        logging_info = dict(
            kafka_broker = settings.KAFKA_BROKER, # Kafka 브로커 리스트
            kafka_topic = settings.KAFKA_TOPIC, # Kafka 토픽
        )
    )
)