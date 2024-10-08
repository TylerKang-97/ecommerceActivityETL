### Application
- CSVProcessor: CSV 파일 처리 및 파티셔닝 작업 수행
- ExternalTableCreator: External Table 생성 및 조회

### 버전
- Scala 2.13.12
- Java 11
- Apache Spark 3.5.3
- Parquet 1.12.3
- Hadoop 3.3.4

### 주요 기능
- CSV 파일 처리
  - CSV 파일을 읽어 event_time 열의 UTC 시간을 KST로 변환하고, date 열을 추가해 일별로 데이터를 파티셔닝하였습니다.

- Parquet로 데이터 저장
  - 변환된 데이터를 Parquet 파일 형식으로 저장하고, 압축 방식은  Snappy 압축을 사용하였습니다.

- 처리 완료 파일 관리
  - 성공적으로 처리된 파일은 success_list.txt에 기록하고, done 디렉터리로 이동시켜 처리된 파일과 처리되지 않은 파일을 구분하였습니다.

- Hive External Table 생성
  - Hive External Table을 생성하고, 새로운 데이터를 추가할 때 MSCK REPAIR TABLE을 실행하여 파티션을 관리하였습니다.

- 배치 장애 복구
  - 배치 작업 도중 장애가 발생할 경우, 해당 파일로부터 생성된 Parquet 파일 및 파티션을 삭제하고 작업을 복구합니다.
 
### 결과
> 데이터가 성공적으로 처리된 경우, Parquet 파일은 $PATH/output 디렉터리에 저장되고, 처리 완료된 파일은 $PATH/done 디렉터리로 이동됩니다.

### 프로젝트 구조
```
.
├── src
│   └── main
│       └── scala
│           ├── CSVProcessor.scala
│           └── ExternalTableCreator.scala
├── data
│   ├── 2019-Oct.csv
│   ├── 2019-Nov.csv
│   ├── success_list.txt
|   ├── done
|   |   └── 처리 완료 파일 
│   └── output
│        └── parquet 파일들
├── README.md
├── build.sbt
└── ...

```

### 주의 사항
- CSV 파일 경로($PATH), Parquet 저장 경로($PATH/output), 처리 완료 파일 경로($PATH/done) 등은 프로젝트 구조를 참고 부탁드립니다.
- 애플리케이션은 정상적으로 작동하지만 sbt run 종료 시 아래와 같은 경고 메시지 또는 버전 이슈가 발생할 수 있습니다.
  - Unable to load native-hadoop library

