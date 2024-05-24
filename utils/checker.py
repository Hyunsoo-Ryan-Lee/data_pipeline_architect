import pandas as pd
import sqlalchemy

class DataTypeChecker:

    def __init__(self):
        self.source_dtype, self.conversion_dtype, self.extract_dtype, self.transform_dtype = None, None, None, None
        self.dtype_info = None

        self.bool, self.int, self.float, self.datetime = None, None, None, None
        self._conversion, self._others = None, None

    def _bool_type(self, patterns: str = 'bit'):
        self.bool = self.source_dtype[self.source_dtype.DATA_TYPE.str.contains(patterns, regex=True)].copy(deep=True)
        self.bool['DATA_TYPE'] = 'float64' # bit type을 True/False 값이 아닌 float 으로 초기 성언 (0.0, 1.0, null)
        # ! bool 선언시 null == False, int64 선언시 null 허용하지 않아 오류, Int64 선언시 bit type의 값이 True/False의 string으로 read 되어 integertype 으로 converting 불가

    def _int_type(self, patterns: str = 'tinyint|smallint|int|bigint'):
        self.int = self.source_dtype[self.source_dtype.DATA_TYPE.str.contains(patterns, regex=True)].copy(deep=True)
        self.int['DATA_TYPE'] = 'float64' # int type을 null 허용하는 float 으로 초기 선언 (float, null)
        # ! int64 선언시 null 허용하지 않아 오류, Int64 선언해도 되지만 안전성을 위해 float으로 초기값 선언

    def _float_type(self, patterns: str = 'float|real|double|NUMBER'):
        self.float = self.source_dtype[self.source_dtype.DATA_TYPE.str.contains(patterns, regex=True)].copy(deep=True)
        self.float['DATA_TYPE'] = 'float64'

    def _datetime_type(self, patterns: str = 'date|datetime|datetime2|DATE|timestamp'):
        self.datetime = self.source_dtype[self.source_dtype.DATA_TYPE.str.contains(patterns, regex=True)].copy(deep=True)
        self.datetime['DATA_TYPE'] = 'datetime64[ns]'

    @staticmethod
    def data_exist_checker(db_connector, query:str):
        
        with db_connector as connected:
            try:
                con = connected.conn
                pd.read_sql(query, con)
                return True
            except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.OperationalError) as e:
                print(e)
                return False

    @staticmethod
    def get_dtype_from_pdf(pdf, dtype_name: str):
        return pd.DataFrame([pdf.dtypes.index, pdf.dtypes.values], index=['COLUMN_NAME', dtype_name]).T

    def set_source_dtype(self, pdf_source_dtype):
        self.source_dtype = pdf_source_dtype
        if self.source_dtype is not None:
            self.set_conversion_dtype()
            self.conversion_dtype = pd.concat([self._conversion, self._others]).sort_index()
        else:
            self.conversion_dtype = None

    def set_conversion_dtype(self):
        _conversion_step_list = ['_bool_type', '_int_type', '_float_type', '_datetime_type']
        for _conversion_step in _conversion_step_list:
            self.__getattribute__(_conversion_step)()
        self._conversion = pd.concat([self.bool, self.int, self.float, self.datetime])
        self._others = self.source_dtype[~self.source_dtype.COLUMN_NAME.isin(self._conversion.COLUMN_NAME.values)]

    def set_extract_dtype(self, pdf_extract_dtype):
        self.extract_dtype = pdf_extract_dtype

    def set_transform_dtype(self, pdf_transform_dtype):
        self.transform_dtype = pdf_transform_dtype

    def get_dtype_info(self, schema_name: str, table_name: str, show_level: int = 2):

        self.dtype_info = pd.DataFrame([[None, None, None, None]], columns = ['SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'COLUMN_INDEX'])

        if self.source_dtype is not None:
            source_dtype_info = \
                pd.merge(self.source_dtype, self.conversion_dtype, on='COLUMN_NAME', how='outer') \
                    .rename( columns = {    'DATA_TYPE_x':'SOURCE_DTYPE', 
                                            'DATA_TYPE_y':'CONVERSION_DTYPE'    } )
        else:
            source_dtype_info = \
                pd.DataFrame([[None, None, None]], columns = ['COLUMN_NAME', 'SOURCE_DTYPE', 'CONVERSION_DTYPE'])

        if show_level == 2:
            self.dtype_info = \
                self.dtype_info \
                    .merge(source_dtype_info, on='COLUMN_NAME', how='outer') \
                    .merge(self.extract_dtype, on='COLUMN_NAME', how='outer') \
                    .merge(self.transform_dtype, on='COLUMN_NAME', how='outer')
        elif show_level == 1:
            self.dtype_info = \
                self.dtype_info \
                    .merge(source_dtype_info, on='COLUMN_NAME', how='outer') \
                    .merge(self.extract_dtype, on='COLUMN_NAME', how='outer')
        else:
            self.dtype_info = \
                self.dtype_info \
                    .merge(source_dtype_info, on='COLUMN_NAME', how='outer')

        self.dtype_info['SCHEMA_NAME'], self.dtype_info['TABLE_NAME'], self.dtype_info['COLUMN_INDEX'] = schema_name, table_name, self.dtype_info.index

        return self.dtype_info.dropna(subset=['COLUMN_NAME'])


class TimeDiffChecker:
    def __init__(self, start_table_timestamp, end_table_timestamp):
        self.start_table_timestamp = start_table_timestamp
        self.end_table_timestamp = end_table_timestamp
        self.set_execution_time_diff()
        
    def set_execution_time_diff(self):
        time_diff = self.end_table_timestamp - self.start_table_timestamp
        self.table_execution_time_diff = time_diff.seconds + round(time_diff.microseconds/1000000, 3)

    def get_execution_time_diff_info(self, schema_name: str, table_name: str):
        self.execution_time_diff_info = pd.DataFrame([[schema_name, table_name]], columns = ['SCHEMA_NAME', 'TABLE_NAME'])
        self.execution_time_diff_info['DURATION'] = self.table_execution_time_diff
        return self.execution_time_diff_info
        

class RowCountChecker:
    
    def __init__(self):
        self.extract_row_count, self.transform_row_count = None, None
        self.row_count_info = None

    def set_extract_row_count(self, pdf_extract_data):
        self.extract_row_count = len(pdf_extract_data)

    def set_transform_row_count(self, pdf_transform_data):
        self.transform_row_count = len(pdf_transform_data)

    def get_row_count_info(self, schema_name: str, table_name: str, show_level: int = 2):

        self.row_count_info = pd.DataFrame([[schema_name, table_name]], columns = ['SCHEMA_NAME', 'TABLE_NAME'])

        if show_level == 2:
            self.row_count_info['EXTRACT_ROW_COUNT'] = self.extract_row_count
            self.row_count_info['TRANSFORM_ROW_COUNT'] = self.transform_row_count
        else:
            self.row_count_info['EXTRACT_ROW_COUNT'] = self.extract_row_count

        return self.row_count_info


class ETLChecker:

    def __init__(self):
        self.total_dtype_info = pd.DataFrame(columns = ['SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'COLUMN_INDEX', 'SOURCE_DTYPE', 'CONVERSION_DTYPE', 'EXTRACT_DTYPE', 'TRANSFORM_DTYPE'])
        self.total_row_count_info = pd.DataFrame(columns = ['SCHEMA_NAME', 'TABLE_NAME', 'EXTRACT_ROW_COUNT', 'TRANSFORM_ROW_COUNT'])
        self.total_time_diff_info = pd.DataFrame(columns = ['SCHEMA_NAME', 'TABLE_NAME', 'DURATION'])

    def set_total_dtype_info(self, dtype_info):
        self.total_dtype_info = pd.concat([self.total_dtype_info, dtype_info]).reset_index(drop=True)
    
    def set_total_row_count_info(self, row_count_info):
        self.total_row_count_info = pd.concat([self.total_row_count_info, row_count_info]).reset_index(drop=True)
    
    def set_total_time_diff_info(self, time_diff_info):
        self.total_time_diff_info = pd.concat([self.total_time_diff_info, time_diff_info]).reset_index(drop=True)
    
    def join_total_count_duration_info(self):
        self.total_table_info = \
            self.total_row_count_info \
                .merge(self.total_time_diff_info, on=['SCHEMA_NAME', 'TABLE_NAME'], how='outer')