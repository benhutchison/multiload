name := "multiload"

version := "0.1"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.0",
  "net.tixxit" %% "delimited-core" % "0.9.0",
  "org.postgresql" % "postgresql" % "42.1.4"
)
