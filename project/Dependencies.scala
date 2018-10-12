import sbt.Keys._
import sbt._

object Dependencies {

  lazy val randomDataProducer = Seq(
    libraryDependencies ++=  Seq(
      "org.apache.kafka" %% "kafka" % "1.0.0",
      "io.confluent" % "kafka-avro-serializer" % "4.0.0",
      "org.apache.avro" % "avro" % "1.8.2",
      "com.typesafe" % "config" % "1.3.3",
      "com.github.pureconfig" %% "pureconfig" % "0.9.2",
      "org.json4s" %% "json4s-native" % "3.6.1",
      "org.json4s" %% "json4s-jackson" % "3.6.1",


  //TEST
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )

}
