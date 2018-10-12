name := "RandomDataProducer"

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.sky",
  scalaVersion := "2.11.6",
  test in assembly := {}
)

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "confluent" at "http://packages.confluent.io/maven/",
  "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

//crossScalaVersions := Seq("2.11.6", "2.12.7")
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false
updateOptions := updateOptions.value.withCachedResolution(true)
logLevel := util.Level.Warn

lazy val root = (project in file("."))
  .enablePlugins(DockerPlugin, DockerComposePlugin)
  .settings(commonSettings: _*)
  .settings(
    assemblyJarName in assembly := s"RandomDataProducer-assembly-0.1-SNAPSHOT.jar",
    mainClass in assembly := Some("ProduceData"),
    Dependencies.randomDataProducer,
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

/*
test in assembly := Seq(
  (test in Test).value
)
*/



