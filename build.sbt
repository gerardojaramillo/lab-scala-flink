import Dependencies.munit

lazy val scalaVer = "2.13.18"
lazy val flinkVer = "2.1.1"
lazy val flinkExtendedVer = "2.2.0"
lazy val flinkKafkaConectorVer = "4.0.1-2.0"

ThisBuild / scalaVersion := scalaVer
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "lab-scala-flink",
    fork := true,
    libraryDependencies ++= Seq(
      "org.flinkextended" %% "flink-scala-api-2" % flinkExtendedVer,
      "org.apache.flink" % "flink-streaming-java" % flinkVer,
      "org.apache.flink" % "flink-clients" % flinkVer,
      "org.apache.flink" % "flink-connector-files" % flinkVer,
      "org.apache.flink" % "flink-csv" % flinkVer,
      "org.apache.flink" % "flink-connector-kafka" % flinkKafkaConectorVer,
      "org.apache.flink" % "flink-connector-jdbc" % "3.2.0-1.19" % Test,
      "org.apache.flink" % "flink-statebackend-rocksdb" % flinkVer % Provided,
      "org.scala-lang" % "scala-reflect" % scalaVer,
      "org.slf4j" % "slf4j-api" % "2.0.13",
      "ch.qos.logback" % "logback-classic" % "1.5.6",
      munit % Test
    ),
    dependencyOverrides += "org.scala-lang" % "scala-reflect" % scalaVer,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
  )
