organization := "eu.inn"

name := "kafka-mimic"

version := "1.0"

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

libraryDependencies ++= Seq(
  "com.typesafe.akka" %%  "akka-actor"      % "2.4.1",
  "com.typesafe.akka" %%  "akka-slf4j"      % "2.4.1",
  "com.typesafe.akka" %%  "akka-testkit"    % "2.4.1"   % "test",
//  "ch.qos.logback"    %   "logback-classic" % "1.1.3",
  "org.scalatest"     %%  "scalatest"       % "2.2.1"   % "test",
  "ru.inn"            %   "jmx-javaagent"   % "1.0",
  "jcifs"             %   "jcifs"           % "1.3.17",
  "org.apache.kafka"  %% "kafka"            % "0.8.2.2" exclude("org.slf4j", "slf4j-log4j12"),
  "eu.inn"            %% "fluentd-scala"    % "0.1.20"
)
