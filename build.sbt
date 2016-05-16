name := "leveldb"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases",
    "RoundEights" at "http://maven.spikemark.net/roundeights"
)

libraryDependencies ++= Seq(
    "com.roundeights" %% "hasher" % "1.2.0",
    "org.scalactic" %% "scalactic" % "2.2.6",
    "org.feijoas" %% "mango" % "0.12",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test")