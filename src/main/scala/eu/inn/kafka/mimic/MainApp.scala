package eu.inn.kafka.mimic

import scala.concurrent.Await
import scala.concurrent.duration._


object MainApp extends App with ComponentRegistry with Logging with Metrics {

  log.info("Starting kafka mimic.")

  Runtime.getRuntime.addShutdownHook(new Thread() { override def run() {
    coordinator ! RequestShutdown
    Await.result(actorSystem.whenTerminated, 15.seconds)
  } })

  startMetricsReporting()

  coordinator ! Start
}
