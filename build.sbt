name := "newday-homework"

version := "1.0"

scalaVersion := "2.12.8"
val specs2Version = "4.2.0"
val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.specs2" %% "specs2-core" % specs2Version % "test",
  "org.specs2" %% "specs2-junit" % specs2Version % "test",
  "org.specs2" %% "specs2-mock" % specs2Version % "test",
  "log4j" % "log4j" % "1.2.17"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}