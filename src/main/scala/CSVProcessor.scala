import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, FileWriter, BufferedWriter}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.sys.ShutdownHookThread

object CSVProcessor {
  @volatile var isShuttingDown = false // 종료 시그널을 처리하기 위한 플래그

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("E-commerce Activity ETL")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // snappy 압축 설정
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    // 스키마 정의
    val customSchema = StructType(Array(
      StructField("event_time", StringType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", StringType, true),
      StructField("category_id", StringType, true),
      StructField("category_code", StringType, true),
      StructField("brand", StringType, true),
      StructField("price", DoubleType, true),
      StructField("user_id", StringType, true),
      StructField("user_session", StringType, true)
    ))

    import spark.implicits._

    val inputPath = "/Users/tyler/Documents/spark/data/job"
    val outputPath = "/Users/tyler/Documents/spark/data/job/output"
    val donePath = "/Users/tyler/Documents/spark/data/job/done"
    val successListPath = "/Users/tyler/Documents/spark/data/job/success_list.txt"

    // Graceful 종료를 위한 Shutdown hook 추가
    val shutdownHook: ShutdownHookThread = sys.addShutdownHook {
      isShuttingDown = true // 종료 플래그 활성화
      println("Graceful shutdown init... Performing cleanup...")
    }

    // success_list.txt에서 이미 처리된 파일 목록 읽기
    val processedFiles = if (new File(successListPath).exists()) {
      scala.io.Source.fromFile(successListPath).getLines().toSet
    } else {
      Set[String]()
    }

    // 새로 추가된 CSV 파일들 리스트업
    val inputFiles = new File(inputPath).listFiles(_.getName.endsWith(".csv")).map(_.getPath)
    val newFiles = inputFiles.filterNot(filePath => processedFiles.contains(filePath))

    if (newFiles.nonEmpty) {
      println(s"Processing new files: ${newFiles.mkString(", ")}")

      newFiles.foreach { file =>
        if (isShuttingDown) {
          println("Shutdown in progress. Stopping file processing.")
          return // 종료 플래그가 활성화되면 파일 처리 중단
        }

        try {
          val df = spark.read
            .option("header", "true")
            .schema(customSchema)
            .csv(file)

          // " UTC" 문자열을 제거한 후 UTC로 변환
          val dfWithCorrectedTime = df.withColumn("event_time_cleaned", regexp_replace($"event_time", " UTC", ""))
            .withColumn("event_time_kst", from_utc_timestamp($"event_time_cleaned", "Asia/Seoul").cast(StringType))

          // KST 기준으로 'date' 컬럼 추가
          val dfWithDate = dfWithCorrectedTime.withColumn("date", date_format($"event_time_kst", "yyyy-MM-dd"))
            .drop("event_time_cleaned") // 임시 컬럼 삭제

          // Parquet 파일로 저장 (일별 파티셔닝)
          dfWithDate.write
            .mode("append")
            .partitionBy("date")
            .parquet(outputPath)

          println(s"Data has been saved to $outputPath with daily partitions.")

          // 성공적으로 처리된 파일명을 success_list.txt에 기록
          val writer = new BufferedWriter(new FileWriter(successListPath, true)) // true는 append 모드
          writer.write(s"${file}\n")
          writer.close()

          // 파일을 Done 디렉터리로 이동
          val source = Paths.get(file)
          val destination = Paths.get(donePath, new File(file).getName)
          Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING)

          println(s"File $file has been moved to $donePath.")

          // External Table에 새로운 파티션 추가
          spark.sql("MSCK REPAIR TABLE user_activity_logs")
          println("New partitions have been added to the external table.")

        } catch {
          case e: Exception =>
            println(s"Error processing file $file: ${e.getMessage}")
            // 실패 시 Parquet 파일 및 파티션 삭제
            cleanUpFailedFile(file, outputPath, spark)
        }
      }
    } else {
      println("No new files to process.")
    }

    // graceful shutdown을 위해 안전하게 Spark 세션 종료
    spark.stop()
    shutdownHook.remove() // 종료 후 shutdown hook 제거
  }

  // 실패한 파일의 Parquet 파일 및 파티션을 삭제하는 함수
  def cleanUpFailedFile(file: String, outputPath: String, spark: SparkSession): Unit = {
    import spark.implicits._
    try {
      // 실패한 파일에서 생성된 parquet 파일 및 파티션 삭제 로직
      val df = spark.read.parquet(outputPath)
      val partitions = df.select("date").distinct().as[String].collect()
      partitions.foreach { partition =>
        val partitionPath = s"$outputPath/date=$partition"
        Files.deleteIfExists(Paths.get(partitionPath))
        println(s"Deleted partition: $partitionPath")
      }
      println(s"Cleanup completed for failed file: $file")
    } catch {
      case e: Exception =>
        println(s"Error during cleanup of failed file $file: ${e.getMessage}")
    }
  }
}



