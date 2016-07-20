organization := "info.exascale"
name := "wdctools"
scalaVersion := "2.10.5"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
version := "1.0"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

//libraryDependencies += "nu.validator.htmlparser" % "htmlparser" % "1.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" //% "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2" //% "provided"

libraryDependencies += "com.netaporter" %% "scala-uri" % "0.4.13"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"

libraryDependencies += "net.debasishg" %% "redisclient" % "3.0"


resolvers += Resolver.sonatypeRepo("public")

initialCommands in console := "import info.exascale._"