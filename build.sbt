name := "apple-coding-challenge"
organization := "com.apple.codingchallenge"
publishMavenStyle := true

scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"

resolvers ++= Seq(
  "Concurrent Maven Repo" at "http://conjars.org/repo"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
       "org.rogach" %% "scallop" % "3.1.2",
       "org.yaml" % "snakeyaml" % "1.21",
       "net.jcazevedo" %% "moultingyaml" % "0.4.0"
    ).map(_.excludeAll(
      ExclusionRule(organization = "org.scalacheck"),
      ExclusionRule(organization = "org.scalactic"),
      ExclusionRule(organization = "org.scalatest"),
      ExclusionRule(organization = "ch.qos.logback"),
      ExclusionRule(organization = "commons-logging"))
    ).map(_.exclude("org.python", "jython-standalone"))
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Test Dependencies
libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.9.0" % "test",
)

test in assembly := {}