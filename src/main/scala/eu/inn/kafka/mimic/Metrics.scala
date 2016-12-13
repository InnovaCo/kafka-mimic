package eu.inn.kafka.mimic

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit
import java.util.{Timer, TimerTask}

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import nl.grons.metrics.scala.{InstrumentedBuilder, MetricName}

object Metrics extends Logging {
  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  def startReporter(host: String, port: Int, prefix: String, reportPeriod: Long) = {

    val graphite = new Graphite(new InetSocketAddress(host, port))

    val reporter = GraphiteReporter.forRegistry(metricRegistry)
      .prefixedWith(prefix + "." + InetAddress.getLocalHost.getHostName.replaceAll("\\.", "-"))
      .build(graphite)

    new Timer("graphite-reporter-timer").schedule(
      new TimerTask { def run() = reporter.report() },
      reportPeriod,
      reportPeriod
    )
  }
}

trait Metrics extends InstrumentedBuilder {
  override lazy val metricBaseName = MetricName("kafka-mimic")

  lazy val metricRegistry = Metrics.metricRegistry
}

trait MetricsComponent extends Metrics with Logging {

  this: ConfigComponent =>

  def startMetricsReporting() = {
    if (config.hasPath("enabled") && config.getBoolean("enabled")) {
      val host = config.getString("host")
      val port = config.getInt("port")
      log.info(s"Starting graphite reporter for $host:$port")
      Metrics.startReporter(
        host,
        port,
        config.getString("prefix"),
        config.getDuration("report-period", TimeUnit.MILLISECONDS)
      )
    }

    metrics.gauge("heartbeat") { 1 }
  }
}