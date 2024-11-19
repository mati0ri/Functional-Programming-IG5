package fr.umontpellier.ig5

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Neo4jTest {
  def main(args: Array[String]): Unit = {
    // Replace with the actual connection URI and credentials
    val url = "neo4j://localhost:7687"
    val username = "neo4j"
    val password = "Mateo3945"
    val dbname = "Project_DB"

    val spark = SparkSession.builder
      .config("neo4j.url", url)
      .config("neo4j.authentication.basic.username", username)
      .config("neo4j.authentication.basic.password", password)
      .config("neo4j.database", dbname)
      .appName("Spark App")
      .master("local[*]")
      .getOrCreate()

    // Charger les données JSON initiales
    val rawData = spark.read.json("data/nvdcve-1.1-2023.jsonl")

    // Transformer les données pour extraire description et ImpactScore
    val transformedData = rawData
      .select(explode(col("CVE_Items")).as("cveItem")) // Éclater CVE_Items en lignes individuelles
      .select(
        col("cveItem.cve.description.description_data")(0)("value").as("description"), // Extraire description
        col("cveItem.impact.baseMetricV3.impactScore").as("ImpactScore") // Extraire ImpactScore
      )
      .filter(col("ImpactScore").isNotNull) // Filtrer les entrées où ImpactScore est non nul

    // Écrire les données transformées dans Neo4j
    transformedData.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("labels", "CVE") // Définir le label pour les nœuds
      .option("node.keys", "description") // Définir la clé unique pour chaque nœud
      .save()

    println("Les données ont été écrites avec succès dans Neo4j.")

    // Lire les données depuis Neo4j pour vérification
    val ds = spark.read
      .format("org.neo4j.spark.DataSource")
      .option("labels", "CVE") // Lire les nœuds avec le label CVE
      .load()

    ds.show(false) // Afficher les données récupérées
  }
}
