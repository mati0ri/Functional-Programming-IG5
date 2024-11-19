package fr.umontpellier.ig5

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ReformatJSON {
  def main(args: Array[String]): Unit = {
    // Initialiser SparkSession
    val spark = SparkSession.builder
      .appName("Reformat JSON")
      .master("local[*]")
      .getOrCreate()

    // Charger les données JSON originales
    val inputPath = "data/nvdcve-1.1-2023.json"
    val outputPath = "data/reformatted.json"

    val rawData = spark.read.json(inputPath)

    // Extraire description et ImpactScore
    val reformattedData = rawData
      .select(explode(col("CVE_Items")).as("cveItem"))
      .select(
        col("cveItem.cve.description.description_data")(0)("value").as("description"),
        col("cveItem.impact.baseMetricV3.impactScore").as("ImpactScore")
      )
      .filter(col("ImpactScore").isNotNull) // Filtrer uniquement les entrées avec un ImpactScore non null

    // Sauvegarder les données reformattées dans un fichier JSON
    reformattedData.write
      .mode(SaveMode.Overwrite)
      .json(outputPath)

    println(s"Data reformatted and saved to $outputPath")

    // Afficher un aperçu des données reformattées
    reformattedData.show(false)

    // Arrêter SparkSession
    spark.stop()
  }
}
