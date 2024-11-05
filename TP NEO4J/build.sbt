ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "fp1",
    idePackagePrefix := Some("fr.umontpellier.ig5")
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
