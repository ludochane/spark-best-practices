name := """spark-demo"""

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.2"
val framelessVersion = "0.2.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion withSources() withJavadoc(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion withSources() withJavadoc(),
  "org.apache.spark" %% "spark-hive" % sparkVersion withSources() withJavadoc(),
  "io.github.adelbertc" %% "frameless-dataset" % framelessVersion
)

