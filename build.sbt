organization := "eu.inn"

name := "kafka-mimic"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:implicitConversions",
  "-feature",
  "-deprecation",
  "-unchecked",
  "-optimise",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

javacOptions ++= Seq(
  "-source", "1.7",
  "-target", "1.7",
  "-encoding", "UTF-8",
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

packSettings

packMain := Map(
  "start" → "-Xmx512m -Dfile.encoding=UTF-8 -Dlogback.configurationFile=${PROG_HOME}/conf/logback.xml -Dconfig.localfile=\"${PROG_HOME}/conf/env.${ENV:-qa}.conf\" eu.inn.kafka.mimic.MainApp"
)

packResourceDir += (baseDirectory.value / "src/main/resources" -> "conf")

packJarNameConvention := "full"

val akkaVersion = "2.4.14"
libraryDependencies ++= Seq(
  "org.typelevel"         %%  "cats"              % "0.8.1",
  "com.typesafe.akka"     %%  "akka-actor"        % akkaVersion,
  "com.typesafe.akka"     %%  "akka-slf4j"        % akkaVersion,
  "ch.qos.logback"        %   "logback-classic"   % "1.1.7",
  "org.apache.kafka"      %%  "kafka"             % "0.8.2.2" exclude("org.slf4j", "slf4j-log4j12"),
  "nl.grons"              %%  "metrics-scala"     % "3.5.5",
  "io.dropwizard.metrics" %   "metrics-graphite"  % "3.1.2",
  "com.typesafe"          %   "config"            % "1.3.1",
  "org.scalatest"         %%  "scalatest"         % "2.2.6"     % "test",
  "com.typesafe.akka"     %%  "akka-testkit"      % akkaVersion % "test"
)
