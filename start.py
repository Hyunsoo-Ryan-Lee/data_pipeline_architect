from main import start_batch
import os, warnings

warnings.filterwarnings('ignore')

def start(**kwargs) -> None:
    start_batch(**kwargs)

if __name__ == '__main__':
    start_values = dict(
        # Kubernetes Job 실행시 들어오는 변수
        EXECUTION_JOB = os.environ.get("EXECUTION_JOB"),
        PARALLELISM = os.environ.get("PARALLELISM"),
        COUNT_INDEX = os.environ.get("COUNT_INDEX")
    )
    
    start(**start_values)