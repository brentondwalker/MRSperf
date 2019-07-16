name         := "spark-arrivals"
version      := "1.0"
organization := "ikt"
scalaVersion := "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core"  % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "2.3.2"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
//libraryDependencies += "commons-cli" % "commons-cli" % "1.2" % "provided"
//libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

resolvers += Resolver.mavenLocal
//mainClass in assembly := Some("forlulator.FJSimulator")

//logLevel := Level.Debug

// because this is NOT a java project
//autoScalaLibrary := false
//crossPaths := false

// if you are using sbt-eclipse, tell it that this is a java project
//EclipseKeys.projectFlavor := EclipseProjectFlavor.Java

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
