name := "integration-webinar"
scalaVersion := "2.12.4"
dependencyOverrides += "com.typesafe.akka" %% "akka-stream" % "2.5.10"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.10"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.18"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-jms" % "0.18"
libraryDependencies += "javax.jms" % "jms" % "1.1"
libraryDependencies += "org.apache.activemq" % "activemq-all" % "5.15.2"
libraryDependencies += "junit" % "junit" % "4.11" % "test"
libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"
libraryDependencies += "org.hamcrest" % "hamcrest-all" % "1.3"
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

test in assembly := {}
assemblyJarName in assembly := "integration.jar"
mainClass in assembly := Some("integration.PresentationDemo")

assemblyMergeStrategy in assembly := {
  case PathList("javax", xs @ _*)            => MergeStrategy.first
  case "application.conf" | "reference.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
