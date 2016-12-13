package eu.inn.kafka.mimic

import org.slf4j.LoggerFactory

trait Logging {
  protected lazy val log = LoggerFactory.getLogger(this.getClass)
}