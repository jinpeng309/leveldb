name := "leveldb"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.2"


libraryDependencies += "org.feijoas" %% "mango" % "0.12"