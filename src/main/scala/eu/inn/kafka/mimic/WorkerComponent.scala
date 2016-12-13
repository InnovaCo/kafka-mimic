package eu.inn.kafka.mimic

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import kafka.consumer.{ConsumerIterator, ConsumerTimeoutException}
import kafka.utils.IteratorTemplate

import scala.util.control.NonFatal

trait WorkerComponent {

  this: ConfigComponent
    with ActorSystemComponent
    with MetricsComponent
    with PublisherComponent ⇒

  def buildWorker(name: String): ActorRef =
    actorSystem.actorOf(Props(classOf[WorkerActor], this), name)

  class WorkerActor extends Actor with Logging {

    def receive = {
      case Assignment(job) ⇒
        context.become(consuming(job).orElse(receive))

      case NextBatch ⇒
        log.error(s"Unable to process next batch. No job for $self")
    }

    def consuming(job: Job): Receive = {
      case NextBatch ⇒
        val it = job.stream.iterator()
        var count = 0

        try {
          while (count < appConfig.batchSize) {
            try {
              val m = it.next()
              publisher.send(job.topicMap(m.topic), m)
              count += 1
            } catch {
              case e: kafka.message.InvalidMessageException if job.skipCorrupted ⇒
                log.warn("Corrupted message. Skipping.", e)
                resetIteratorState(it)
            }
          }
        } catch {
          case e: ConsumerTimeoutException ⇒
          case NonFatal(e) ⇒
            sender ! e
        }

        metrics.meter("messages").mark(count)

        sender ! BatchCompleted

      case PoisonPill ⇒
        log.debug(s"Worker $job is closing")
    }

    private def resetIteratorState(it: ConsumerIterator[Array[Byte], Array[Byte]]): Unit = {
      val method = classOf[IteratorTemplate[_]].getDeclaredMethod("resetState")
      method.setAccessible(true)
      method.invoke(it)
    }
  }
}
