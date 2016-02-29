name := "feedTransform"

version := "1.0"

scalaVersion := "2.10.5"


scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided"
libraryDependencies += "com.netaporter" %% "scala-uri" % "0.4.13"

resolvers += Resolver.sonatypeRepo("public")
