lazy val root = (project in file(".")).
    settings(
      name := "testdb",
      version := "1.0.0",
      scalaVersion := "2.11.4"
    )

libraryDependencies ++= Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5",
    "org.scalatest" %% "scalatest" % "2.2.4")

fork := true
javaOptions ++= Seq("-Xms2048M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
