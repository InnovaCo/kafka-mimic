package eu.inn.kafka.mimic

import kafka.consumer.KafkaStream

case class Job(
                stream: KafkaStream[Array[Byte], Array[Byte]],
                topicMap: Map[String, String],
                skipCorrupted: Boolean
              )