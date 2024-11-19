package fr.umontpellier.ig5

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Neo4jApp {
  def main(args: Array[String]): Unit = {
    // Configuration Spark et Neo4j
    val url = "neo4j://localhost:7687"
    val username = "neo4j"
    val password = "Mateo3945"
    val dbname = "Project_DB"

    val spark = SparkSession.builder
      .appName("Neo4j Data Transformation")
      .master("local[*]")
      .config("neo4j.url", url)
      .config("neo4j.authentication.basic.username", username)
      .config("neo4j.authentication.basic.password", password)
      .config("neo4j.database", dbname)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawData = spark.read
      .option("multiline", "true")
      .json("data/nvdcve-1.1-2024.json")

    // Inspecter la structure des données
    rawData.printSchema()
    rawData.show(false)

    // Transformer les données pour extraire description et ImpactScore
    val transformedData = rawData
      .select(explode(col("CVE_Items")).as("cveItem")) // Diviser les CVE_Items
      .select(
        col("cveItem.cve.CVE_data_meta.ID").as("CVE_ID"), // Extraire l'ID
        col("cveItem.cve.description.description_data")(0)("value").as("description"), // Extraire la description
        col("cveItem.impact.baseMetricV3.cvssV3.baseScore").as("BaseScore") // Extraire le score de base
      )
      .filter(col("BaseScore").isNotNull) // Filtrer les entrées avec BaseScore non nul

    // Écrire les données transformées dans Neo4j
    transformedData.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("labels", "CVE") // Label pour les nœuds
      .option("node.keys", "CVE_ID") // Utiliser l'ID comme clé unique
      .save()

    println("Les données ont été transformées et écrites avec succès dans Neo4j.")

    // Lire les données depuis Neo4j pour vérification
    val neo4jData = spark.read
      .format("org.neo4j.spark.DataSource")
      .option("labels", "CVE")
      .load()

    println("Données récupérées depuis Neo4j :")
    neo4jData.show(false) // Afficher les données depuis Neo4j
  }
}
