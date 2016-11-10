scalaVersion := "2.11.8"

organization := "com.handy"

name := "schema"

version := "0.0.2"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "org.apache.hive" % "hive-serde" % "1.2.1",
  "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided",
  "com.google.guava" % "guava" % "16.0.1",
  "io.circe" %% "circe-core" % "0.5.1",
  "io.circe" %% "circe-parser" % "0.5.1",
  "org.typelevel" %% "cats" % "0.7.0",
  "org.specs2" %% "specs2-core" % "3.8.3" % "test",
  "org.tpolecat" %% "atto-core"  % "0.5.0",
  "com.github.mpilquist" %% "simulacrum" % "0.10.0"
)

mainClass in (Compile, packageBin) := Some("com.handy.App")

scalacOptions in Test ++= Seq("-Yrangepos")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

crossScalaVersions := Seq("2.10.6")
