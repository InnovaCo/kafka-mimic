package eu.inn.kafka.mimic.tools

import java.util.Properties

import com.typesafe.config.Config

import scala.collection.JavaConversions._


object Utils {

  def buildProperties(config: Config, more: (String, String)*) = {
    config.entrySet
      .map(kv ⇒ (kv.getKey, kv.getValue.unwrapped.toString))
      .union(more.toSet)
      .foldLeft(new Properties) { (p, kv) ⇒ p.put(kv._1, kv._2); p }
  }

  @annotation.tailrec
  def retry[T](n: Int, onException: Throwable ⇒ Unit)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) ⇒ x
      case util.Failure(e) if n > 1 ⇒
        onException(e)
        retry(n - 1, onException)(fn)
      case util.Failure(e) => throw e
    }
  }
}
