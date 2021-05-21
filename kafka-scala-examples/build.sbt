
name := "kafka-scala-examples"

version := "0.1"

scalaVersion := "2.12.13"

//idePackagePrefix := Some("datakickstart.kafka")

//val sparkVersion = "3.1.0"

resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io/maven/",
  MavenRepo("confluent", "https://packages.confluent.io/maven/")
)

//assemblyJarName in assembly := "kafka-training-0.1.jar"
//
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}

lazy val kafkaDeps = Seq(
  "org.apache.kafka" %% "kafka" % "2.1.0",
  "io.confluent" % "kafka-avro-serializer" % "5.5.1"
)

// This section adds all the dependencies from our list
libraryDependencies ++= kafkaDeps