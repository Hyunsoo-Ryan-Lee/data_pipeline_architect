## 1. 파이프라인 모듈 구성
```
.
├── start.py
├── main.py
├── settings.py
│
├── jobConfigs
│   ├── dev
│   │   └── database_dev_a_jobConfig.yaml
│   └── prd
│       └── database_prd_a_jobConfig.yaml
│
├── db
│   └── temp
│       ├── dev
│       │   └── database_dev_a.py
│       └── prd
│           └── database_prd_a.py
│
├── jobs
│   └── temp
│       ├── dev
│       │   └── database_dev_a_job.py
│       └── prd
│           └── database_prd_a_job.py
│
├── pipeline
│   ├── controller.py
│   ├── 01_skeleton.py
│   ├── 02_extract.py
│   ├── 03_load.py
│   ├── 04_update.py
│   └── 05_send.py
│
└── utils
    ├── checker.py
    ├── connector.py
    ├── logger.py
    └── utils.py
```
---
## 2. 파이프라인 모듈별 기능 정의

- start.py - 파이프라인 모듈의 시작 지점으로 실행에 필요한 최소 정보들을 받아 main.py 호출
- main.py - 이후 파이프라인 작동에 필요한 정보들을 json 형태로 만들어 execution plan 생성
- settings.py - 각종 고정 정보들에 대한 모음 파일 (접속정보, 디렉토리, OBJ 버킷 위치 등)
- jobConfigs/ - Kubernetes 상에서 파이프라인 작업 실행을 위한 작업 정의
- db/ - 데이터베이스별 이행 쿼리가 들어있는 디렉토리
- pipeline/ - 파이프라인 실행 관련 코드 모음 디렉토리
- controller.py - 전체 ETL 작업에 대한 흐름을 순서대로 제어하는 함수 
  - 01_skeleton.py - extract, load, update, send 각 기능별 실행 함수가 모듈화 되어 정리된 파일
  - 02_extract.py - 연결된 데이터베이스에 SELECT 쿼리를 요청하여 데이터를 추출하는 함수 (쿼리 → Pandas DF 생성)
  - 03_load.py - 추출된 데이터를 적재 규칙에 맞게 Naver Cloud OBJ에 저장하는 함수
  - 04_update.py - OBJ에 적재된 데이터를 바탕으로 Trino 테이블을 생성하고 최신 데이터로 업데이트 시켜주는 함수
  - 05_send.py - 최종 ETL 결과에 대한 테이블을 Json화 하여 Elastic Search Logging으로 전송해주는 함수
- utils/ -  파이프라인 실행에 필요한 세부적인 기능들에 대한 모음
  - checker.py - 데이터 타입, 실행 시간, 이행 Row 수 등 ETL 내용 확인을 위한 함수들 모음
  - connector.py - DB connection 생성을 위한 함수 모음
  - logger.py - Log 내용을 Elastic Search로 전송하기 위한 Kafka Producer 세팅 및 실행 함수별 세부 Logging 모듈
  - utils.py - 날짜 생성, 쿼리 파싱 등 모듈 작동에 필요한 부가적인 기능을 하는 함수들 모음
---
## 3. 데이터 적재 디렉토리 규칙
> s3://[BUCKET 이름]/temp_bronze/[데이터 분류]/[DB 이름]/[TABLE 이름]/yyyy=0000/mm=00/dd=00/[TABLE 이름].parquet
---
## 4. 데이터 타입 변환 Flow

| Data Source     | Pandas DF         | Cloud Storage | Trino           |
|-----------------|-------------------|---------------|-----------------|
| bool            | float64 → Int64   | Int64         | BOOLEAN         |
| int             | float64 → Int64   | Int64         | INT / BIGINT    |
| float           | float64           | float64       | DOUBLE          |
| timestamp       | datetime64        | datetime64    | TIMESTAMP       |
| varchar         | object            | object        | VARCHAR         |
