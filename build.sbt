name := "sparkTransformer"
ThisBuild / useCoursier := false

version := "0.1"

scalaVersion := "2.11.11"
//scalaVersion := "2.12.10"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  //"org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.typesafe.play" % "play-json_2.11" % "2.4.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  //"org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "mysql" % "mysql-connector-java" % "8.0.21"



   excludeAll(
    ExclusionRule(organization = "org.spark-project.spark", name = "unused"),
    ExclusionRule(organization = "org.apache.spark", name = "spark-streaming"),
    ExclusionRule(organization = "org.apache.hadoop")
  )
)

target in assembly := file("build")

assemblyJarName in assembly := s"${name.value}.jar"


assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first

}