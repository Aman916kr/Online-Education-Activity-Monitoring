ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "online-education-producer",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "4.3.1"
    )
  )