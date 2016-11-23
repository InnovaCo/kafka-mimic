package eu.inn.kafka.mimic.tools

import scala.util.Try

class TryIterator[A](in: Iterator[A]) extends Iterator[Try[A]] {

  override def hasNext: Boolean = in.hasNext

  override def next(): Try[A] = Try(in.next)
}
