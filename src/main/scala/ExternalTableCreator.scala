import org.apache.spark.sql.SparkSession

object ExternalTableCreator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("E-commerce Activity ETL")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val outputPath = "/PATH/output"

    // External Table 생성 SQL
    val createTableSQL = s"""
    CREATE EXTERNAL TABLE IF NOT EXISTS user_activity_logs (
      event_time STRING,
      event_type STRING,
      product_id STRING,
      category_id STRING,
      category_code STRING,
      brand STRING,
      price DOUBLE,
      user_id STRING,
      user_session STRING,
      event_time_kst STRING
    )
    PARTITIONED BY (date STRING)
    STORED AS PARQUET
    LOCATION '$outputPath'
    """

    // External Table 생성
    spark.sql(createTableSQL)
    println("External Table 생성 완료")

    // External Table 파티션 리페어
    spark.sql("MSCK REPAIR TABLE user_activity_logs")

    // 파티션을 확인하는 쿼리 실행
    spark.sql("SHOW PARTITIONS user_activity_logs").show(100, false)

    // 테이블에서 데이터 조회
    spark.sql("SELECT * FROM user_activity_logs ORDER BY date DESC LIMIT 20").show(false)

    // Spark 세션 종료
    spark.stop()
  }
}

