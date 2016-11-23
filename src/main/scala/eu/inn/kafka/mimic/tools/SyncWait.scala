package eu.inn.kafka.mimic.tools

import java.util.concurrent.{CountDownLatch, TimeUnit}

class SyncWait {
  private val sync = new CountDownLatch(1)

  def set() = sync.countDown()

  def isSet = sync.getCount == 0

  def await(ms: Long): Boolean = sync.await(ms, TimeUnit.MILLISECONDS)
}
