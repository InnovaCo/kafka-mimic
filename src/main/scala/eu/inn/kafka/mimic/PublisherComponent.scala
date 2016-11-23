package eu.inn.kafka.mimic

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, Future}

import eu.inn.util.Logging
import eu.inn.util.metrics.StatsComponent
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

trait PublisherComponent {

  this: AppConfigComponent
    with ActorSystemComponent
    with StatsComponent ⇒

  val publisher = new KafkaPublisher

  class KafkaPublisher extends Logging {

    private lazy val producer = new KafkaProducer[Array[Byte], Array[Byte]](appConfig.producerConfig)

    private val latest = new ConcurrentHashMap[TopicPartition, Future[_]]

    private val failed = new AtomicBoolean(false)

    private val exception = new AtomicReference[Throwable]

    def send(topic: String, message: MessageAndMetadata[Array[Byte], Array[Byte]]) = {

      val partition = appConfig.partitioner.resolvePartition(message.key(), message.partition, producer.partitionsFor(topic).size())
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, message.key(), message.message())

      val tp = new TopicPartition(topic, partition)
      latest += tp → producer.send(record, new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit =
          if (e != null) {
            exception.compareAndSet(null, e)
            failed.set(true)
          }
      })
    }

    def close() =
      producer.close()

    def hasFailed = failed.get()

    def waitForCompletion() = {
      log.trace("Flushing started.")
      while (latest.values.exists(!_.isDone) && !failed.get()) {
        // no-op
      }
      log.trace("Flushing completed.")

      if (failed.get()) {
        throw exception.get()
      }
    }
  }

}