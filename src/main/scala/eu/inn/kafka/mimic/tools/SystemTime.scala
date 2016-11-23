package eu.inn.kafka.mimic.tools

import java.util.Date

object SystemTime {
  private val defaultTimeFunc = () => new Date().getTime

  // todo: sync access?
  var timeFunc: () => Long = defaultTimeFunc

  def now: Long = timeFunc()

  def reset(): Unit = timeFunc = defaultTimeFunc

  def shift(valueMs: Long): Unit = {
    val oldFunc = timeFunc
    timeFunc = () => oldFunc() + valueMs
  }

  def freeze(timestamp: Long): Unit = timeFunc = () => timestamp

  def freeze(): Unit = freeze(now)
}
