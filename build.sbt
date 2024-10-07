import scala.collection.Seq
import sbt.Keys.javaOptions

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerceActivityETL",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",
      "org.apache.spark" %% "spark-hive" % "3.5.3",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",  // 이 부분이 중요합니다
      "org.apache.hadoop" % "hadoop-common" % "3.3.4"
    ),
    ThisBuild / javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
