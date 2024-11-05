package fr.umontpellier.ig5

import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "data/README.md"
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("Spark")).count()
    val numBs = logData.filter(line => line.contains("Scala")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}