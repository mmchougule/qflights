ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "untitled",
    idePackagePrefix := Some("org.quantexa.flights")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "org.mockito" %% "mockito-scala" % "1.16.23" % Test
)