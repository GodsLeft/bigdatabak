name := "SparkTest"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.3")
    