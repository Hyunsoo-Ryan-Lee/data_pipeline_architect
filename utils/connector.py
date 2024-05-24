from dataclasses import dataclass
import sqlalchemy
import trino

@dataclass
class DBConnector:
    conn_name: str
    conn_engine: str
    conn_params: str
    
    def __post_init__(self):
        
        if self.conn_engine == 'TRINO':
            try:
                self.connect = self._trino_connect()
            except RuntimeError:
                raise RuntimeError(f"The '{self.conn_engine}' is not supported at '{self.conn_name}'.")
        else:
            try:
                self.connect = self._engine_connect()
            except RuntimeError:
                raise RuntimeError(f"The '{self.conn_engine}' is not supported at '{self.conn_name}'.")
    
    def __str__(self):
        return f"{self=}"
    
    def __enter__(self):
        self.cursor = self.conn.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()

    def _engine_connect(self):
        self.conn = sqlalchemy.create_engine(self.conn_params)
        
    def _trino_connect(self):
        self.conn = trino.dbapi.connect(**self.conn_params)