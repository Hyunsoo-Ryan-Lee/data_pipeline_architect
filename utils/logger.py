from kafka import KafkaProducer
from atlassian import Jira
import pandas as pd
from dataclasses import dataclass
import inspect, json, os

@dataclass
class MessageProducer:
    broker: list[str]
    topic: str
    def __post_init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        )

    def send_message(self, msg):
        future = self.producer.send(self.topic, msg)
        self.producer.flush()
        future.get(timeout=60)
        return {'status_code': 200, 'error': None}

@dataclass
class JiraIssuer:
    jira_auth: dict
    def __post_init__(self):
        self.jira = Jira(**self.jira_auth)
        
    def issue_comment(self, issue_key, msg):
        self.jira.issue_add_comment(issue_key, msg)

        
@dataclass
class Logging:
        
    @staticmethod
    def get_function_name():
        frame = inspect.currentframe().f_back
        caller_name = frame.f_code.co_name
        return caller_name
    
    @staticmethod
    def excel_writer(path: str, execution_job:str, data: list):
        _path = os.path.join(path, f"{execution_job}.xlsx")
        with pd.ExcelWriter(_path, mode="w", engine="openpyxl") as writer:
            [df.to_excel(writer, sheet_name=f"info_{idx+1}", index=False) for idx, df in enumerate(data)]