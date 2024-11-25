//package fr.umontpellier.ig5
//
//import org.apache.spark.sql.SparkSession
//
//object SimpleApp {
//  def main(args: Array[String]): Unit = {
//    val logFile = "data/README.md"
//    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//
//    val logData = spark.read.textFile(logFile).cache()
//    val numAs = logData.filter(line => line.contains("Spark")).count()
//    val numBs = logData.filter(line => line.contains("Scala")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    spark.stop()
//  }
//}

package fr.umontpellier.ig5
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
object Neo4jTestRead {
  def main(args: Array[String]): Unit = {
    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Replace with the actual connection URI and credentials
    val url = "neo4j+s://c924c8b8.databases.neo4j.io"
    val password = "slBPuTRz_8RtypnuxYQEa-DDbRqoPAxTvRyggM8BBF0"


    val username = "neo4j"
    val dbname = "neo4j"

    val spark = SparkSession.builder
      .config("neo4j.url", url)
      .config("neo4j.authentication.basic.username", username)
      .config("neo4j.authentication.basic.password", password)
      .config("neo4j.database", dbname)
      .appName("Spark App")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    //    // Read from Neo4j
    //    val ds = spark.read
    //      .format("org.neo4j.spark.DataSource")
    //      .option("labels", "Person")
    //      .load()
    //    ds.show()
    val readQuery =
      //      """
      //      MATCH (n:Person)
      //      RETURN id(n) AS id, n.name AS Name
      //      """
      """
  MATCH (n)
  RETURN COUNT(n)
  """

    val df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option("query", readQuery)
      .load()

    df.show()

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    println("\n.......Program *****Completed***** Successfully.....!\n")
    spark.stop()
  }
}