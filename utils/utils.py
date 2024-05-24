import os, re, importlib
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import settings

class DateHandler:

    @staticmethod
    def get_timestamp():
        KST = timezone(timedelta(hours=9))
        return datetime.now(KST)
        
    @staticmethod
    def get_execution_date(execution_interval: str, partition_delta: int):        
        current_date = datetime.now()
        if execution_interval == 'day':
            return datetime.strftime(  (current_date + relativedelta(days = partition_delta))  ,  '%Y%m%d'  )
        elif execution_interval == 'month':
            return datetime.strftime(  (current_date + relativedelta(months = partition_delta))  ,  '%Y%m'  )
        elif execution_interval == 'year':
            return datetime.strftime(  (current_date + relativedelta(years = partition_delta))  ,  '%Y'  )
        else: 
            raise RuntimeError(f"execution_interval '{execution_interval}' is not allowed.")

    @staticmethod
    def get_custom_date(execution_interval: str, custom_date: str):
        try:
            if (execution_interval == 'day') & (len(custom_date) == 8): 
                datetime.strptime(custom_date, '%Y%m%d')
                return custom_date
            elif (execution_interval == 'month') & (len(custom_date) == 6): 
                datetime.strptime(custom_date, '%Y%m')
                return custom_date
            elif (execution_interval == 'year') & (len(custom_date) == 4): 
                datetime.strptime(custom_date, '%Y')
                return custom_date
            else:
                raise RuntimeError(f"execution_interval is '{execution_interval}', but length of custom_date is '{len(custom_date)}'.")
        except Exception as e:
            raise e

    @staticmethod
    def get_dates_range_daily_execution_date_list(execution_date: str, start_date: str = None, past_days: int = None):
        if (start_date is not None) & (past_days is None):
            _date_diff = (  datetime.strptime(execution_date, '%Y%m%d').date() - datetime.strptime(start_date, '%Y%m%d').date()  )
            _past_days = _date_diff.days + 1
        elif (start_date is None) & (past_days is not None):
            _past_days = past_days
        else:
            _past_days = 1
        return [  (  datetime.strptime(execution_date, '%Y%m%d').date() + relativedelta(days = -i)  ).strftime('%Y%m%d') for i in range(_past_days)  ]

    @staticmethod
    def get_dates_range_monthly_execution_date_list(execution_date: str, start_date: str = None, past_months: int = None):
        if (start_date is not None) & (past_months is None):
            _date_diff = relativedelta(  datetime.strptime(execution_date, '%Y%m').date(), datetime.strptime(start_date, '%Y%m').date()  )
            _past_months = (_date_diff.years * 12) + _date_diff.months + 1
        elif (start_date is None) & (past_months is not None):
            _past_months = past_months
        else:
            _past_months = 1
        return [  (  datetime.strptime(execution_date, '%Y%m').date() + relativedelta(months = -i)  ).strftime('%Y%m') for i in range(_past_months)  ]



class QueryHandler:

    @staticmethod
    def get_select_clause(query: str):
        # SELECT ~ FROM 사이의 문장 추출 (regexp = r'SELECT(.*?)FROM')
        # Sub Query 및 Query 출력시 SELECT 되는 컬럼 선언위치 고려하여 search 사용
        select_clause = re.search(r'SELECT(.*?)FROM', query, re.DOTALL | re.IGNORECASE)
        if select_clause is not None:
            return select_clause.group(1)
        else:
            raise RuntimeError(f"The query format is not allowed.")

    @staticmethod
    def get_column_names(select_clause: str):
        # SELECT 문장 내 column_name 추출 (regexp = r'AS\s+(\w+)(?:\s+--.*)?')
        # Non-capturing Group 이용하여 -- 형태의 SQL 주석 제거
        return re.compile(r'AS\s+(\w+)(?:\s+--.*)?', re.IGNORECASE).findall(select_clause)        


class TrinoQueryHandler:
    @staticmethod 
    def schema_check_query(catalog_name, schema_name):
        # Trino Catalog에 schema_name과 동일한 schema가 이미 존재하는지 체크하는 쿼리
        schema_check_query = f"""
        SHOW SCHEMAS FROM {catalog_name} LIKE '{schema_name}'
        """
        return schema_check_query

    @staticmethod 
    def schema_create_query(catalog_name, schema_name):
        schema_create_query = f"""
        CREATE SCHEMA {catalog_name}.{schema_name}
        """
        return schema_create_query
    
    @staticmethod 
    def table_check_query(catalog_name, schema_name, table_name):
        # Trino Metadata에 table_name과 동일한 테이블이 이미 존재하는지 체크하는 쿼리
        metatable_exist_query = f"""
        SHOW TABLES FROM {catalog_name}.{schema_name} LIKE '{table_name}'
        """
        return metatable_exist_query
    
    @staticmethod
    def table_update_query(catalog_name, schema_name, table_name):
        # 이행으로 인해 특정 table의 데이터가 추가되었을 때 데이터를 업데이트 해주는 쿼리
        metadata_update_query = f"""
        CALL {catalog_name}.system.sync_partition_metadata('{schema_name}', '{table_name}', 'ADD')
        """
        return metadata_update_query
    
    @staticmethod
    def table_create_query( pdf_data,
                            parent_data,
                            execution_interval,
                            catalog_name,
                            schema_name,
                            table_name ):
        # Trino Metadata에 table_name에 해당하는 테이블이 없을 때, 새로 테이블을 생성하는 쿼리
        partitioned_date = ['yyyy', 'mm', 'dd'] if execution_interval == 'day' else ['yyyy', 'mm']
        
        # int64 컬럼에서 max 데이터 크기가 2^31-1 보다 크면 BIGINT 형으로 바꿔주는 로직을 위한 부분
        int64_cols = pdf_data.select_dtypes(include=['int64'])
        df_int64 = pdf_data[int64_cols.columns].dropna(axis=1, how='all')
        int64_max = dict(df_int64.max())
        df_int64_max_cols = [i for i in int64_max if int64_max[i]>(2**31-1)]
        
        table_columns = ""
        
        for col in pdf_data.columns:
            
            if col in df_int64_max_cols:
                table_columns += f"{col} BIGINT,\n"
                continue
            
            _dtypes = str(pdf_data[col].dtypes)
            _index = _dtypes.find('64')
            
            if _index != -1:
                _dtypes = _dtypes[:_index].lower()

            table_columns += f"{col} {settings.TYPE_MAPPER[_dtypes]},\n"
        table_columns += ",\n".join([f"{i} VARCHAR" for i in partitioned_date])
        table_columns = table_columns.replace(',\norder ', ',\n"order" ')
        
        metadata_create_query = settings.TRINO_CREATE.format(
            catalog_name = catalog_name,
            schema_name = schema_name,
            table_name = table_name,
            table_columns = table_columns,
            parent_data = parent_data,
            partitioned_date = partitioned_date
        )
        return metadata_create_query

class QuerySelector:
    
    def __init__(self, search_path: str, execution_database: str, query_file: str):
        self.search_path = search_path
        self.execution_database = execution_database
        self.query_file = query_file

        self.location_name = None
        self.query_module = None
        self.queries = None
        
    def get_location_name(self):
        for root, _, files in os.walk(self.search_path):
            if self.execution_database in root:
                files = [f.replace(".py", "") for f in files if "python" not in f]
                if not files:
                    continue
                for file in files:
                    if self.query_file == file:
                        self.location_name = root
                        return self.location_name
                    
    def get_query_module(self):
        self.get_location_name()
        if self.location_name is not None:
            file_path = os.path.join(self.location_name, f"{self.query_file}.py")
            if os.path.isfile(file_path):
                spec = importlib.util.spec_from_file_location(self.query_file, file_path)
                self.query_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self.query_module)        
        return self.query_module

    def get_queries(self):
        self.get_query_module()
        if hasattr(self.query_module, 'queries') and isinstance(self.query_module.queries, dict):
            self.queries = self.query_module.queries
            return self.queries