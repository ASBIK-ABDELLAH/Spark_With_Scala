name := "Spark_With_Scala"

version := "0.1"

scalaVersion := "2.12.0"


val sparkcore = "org.apache.spark" %% "spark-core" % "3.0.1"

val sparksql= "org.apache.spark" %% "spark-sql" % "3.0.1"

lazy val global = project.in(file(".")).aggregate(proj1,proj2,proj3)

lazy val proj1 = project.settings(
  name := "proj1", libraryDependencies ++=Seq(sparkcore,sparksql)
).enablePlugins(AssemblyPlugin)

lazy val proj2 = project.settings(
  name := "proj2", libraryDependencies ++=Seq(sparkcore,sparksql)
).enablePlugins(AssemblyPlugin)

lazy val proj3 = project.settings(
  name := "proj3", libraryDependencies ++=Seq(sparkcore,sparksql)
).enablePlugins(AssemblyPlugin)