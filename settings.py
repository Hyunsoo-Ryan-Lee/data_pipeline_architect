import os, dotenv
from urllib.parse import quote_plus
from atlassian import Jira
from trino.auth import BasicAuthentication

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

## TEMP DEV ##
TEMP_DEV_ENGINE = os.environ.get("TEMP_DEV_ENGINE", "")
TEMP_DEV_HOST = os.environ.get("TEMP_DEV_HOST", "")
TEMP_DEV_USER = os.environ.get("TEMP_DEV_USER", "")
TEMP_DEV_PASSWORD = quote_plus(os.environ.get("TEMP_DEV_PASSWORD", ""))
TEMP_DEV_DATABASE = os.environ.get("TEMP_DEV_DATABASE", "")
TEMP_DEV_PORT = 3306

## TEMP PRD ##
TEMP_PRD_ENGINE = os.environ.get("TEMP_PRD_ENGINE", "")
TEMP_PRD_HOST = os.environ.get("TEMP_PRD_HOST", "")
TEMP_PRD_USER = os.environ.get("TEMP_PRD_USER", "")
TEMP_PRD_PASSWORD = os.environ.get("TEMP_PRD_PASSWORD", "")
TEMP_PRD_DATABASE = os.environ.get("TEMP_PRD_DATABASE", "")
TEMP_PRD_PORT = 3306

## TRINO DEV ##
TRINO_DEV_HOST = os.environ.get("TRINO_DEV_HOST", "")
TRINO_DEV_USER = os.environ.get("TRINO_DEV_USER", "")
TRINO_DEV_PASSWORD = os.environ.get("TRINO_DEV_PASSWORD", "")
TRINO_DEV_CATALOG = os.environ.get("TRINO_DEV_CATALOG", "")
TRINO_DEV_SCHEMA = os.environ.get("TRINO_DEV_SCHEMA", "")

## TRINO PRD ##
TRINO_PRD_HOST = os.environ.get("TRINO_PRD_HOST", "")
TRINO_PRD_USER = os.environ.get("TRINO_PRD_USER", "")
TRINO_PRD_PASSWORD = os.environ.get("TRINO_PRD_PASSWORD", "")
TRINO_PRD_CATALOG = os.environ.get("TRINO_PRD_CATALOG", "")
TRINO_PRD_SCHEMA = os.environ.get("TRINO_PRD_SCHEMA", "")

## TRINO ECT ##
TYPE_MAPPER = {
    'bool' : 'BOOLEAN',
    'datetime' : 'TIMESTAMP',
    'object' : 'VARCHAR',
    'int' : 'INT',
    'float' : 'DOUBLE'
}

TRINO_CREATE = """
CREATE TABLE {catalog_name}.{schema_name}.{table_name}(
{table_columns}
) 
WITH( external_location = '{parent_data}',
      format = 'PARQUET',
      partitioned_by = ARRAY{partitioned_date} )
"""

## CLOUD ##
BUCKET_NAME =  os.environ.get('NCP_BUCKET_NAME', "")
S3_STORAGE = f"s3a://{BUCKET_NAME}/temp_bronze"
NCP_AUTH = dict(
        endpoint_url = os.environ.get('ENDPOINT_URL', ""),
        key = os.environ.get('NCP_ACCESS_KEY_ID', ""),
        secret = os.environ.get('NCP_SECRET_ACCESS_KEY', ""),
    )

## KAFKA ##
KAFKA_BROKER = ["100.200.123.6:9092", "100.200.123.7:9092", "100.200.123.8:9092"]
KAFKA_TOPIC = 'data_topic'

## JIRA ##
JIRA_AUTH = Jira(
    url=os.environ.get('JIRA_URL', ""),
    username=os.environ.get('JIRA_USERNAME', ""),
    password=os.environ.get('JIRA_PASSWORD', "")
    )

JIRA_ISSUE = os.environ.get('JIRA_ISSUE', "")

BASE_DIR = "/mnt/data-pipeline"
LOCAL_LOG_DIR = os.path.join(BASE_DIR, "_log")

FUNCTION_GROUP = {
    "D_SOURCEtoBRONZE_temp_dev" : "TEMP_DEV",
    "D_SOURCEtoBRONZE_temp_prd" : "TEMP_PRD",
}


DB_SETTINGS = dict(
    #################### TEMP ####################
    temp_dev = {
        'location' : '',
        'conn_params' : f"{TEMP_DEV_ENGINE}://{TEMP_DEV_USER}:{TEMP_DEV_PASSWORD}@{TEMP_DEV_HOST}:{int(TEMP_DEV_PORT)}/{TEMP_DEV_DATABASE}"     
        },
    temp_prd = {
        'location' : '',
        'conn_params' : f"{TEMP_DEV_ENGINE}://{TEMP_DEV_USER}:{TEMP_DEV_PASSWORD}@{TEMP_DEV_HOST}:{int(TEMP_DEV_PORT)}/{TEMP_DEV_DATABASE}"     
        },
    #################### TRINO ####################
    trino_dev = {
        'conn_name' : 'trino_dev',
        'conn_engine' : 'TRINO',
        'conn_params' : {
            'host' : TRINO_DEV_HOST,
            'port' : 443,
            'user' : TRINO_DEV_USER,
            'http_scheme' : "https",
            'catalog' : TRINO_DEV_CATALOG,
            'schema' : TRINO_DEV_SCHEMA,
            'auth' : BasicAuthentication(TRINO_DEV_USER, TRINO_DEV_PASSWORD),
        },
    },
    trino_prd = {
        'conn_name' : 'trino_prd',
        'conn_engine' : 'TRINO',
        'conn_params' : {
            'host' : TRINO_PRD_HOST,
            'port' : 443,
            'user' : TRINO_PRD_USER,
            'http_scheme' : "https",
            'catalog' : TRINO_PRD_CATALOG,
            'schema' : TRINO_PRD_SCHEMA,
            'auth' : BasicAuthentication(TRINO_PRD_USER, TRINO_PRD_PASSWORD),
        },
    }
)