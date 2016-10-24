scalaVersion := "2.11.8"

name := "scala-hadoop"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "org.apache.hive" % "hive-serde" % "1.2.1",
  "com.google.guava" % "guava" % "16.0.1",
  "org.tpolecat" %% "atto-core"  % "0.4.2",
  "com.github.mpilquist" %% "simulacrum" % "0.10.0",
  "io.circe" %% "circe-core" % "0.5.1",
  "org.typelevel" %% "cats" % "0.7.0",
  "org.apache.avro" % "avro" % "1.8.1"
)

mainClass in (Compile, packageBin) := Some("com.handy.App")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
