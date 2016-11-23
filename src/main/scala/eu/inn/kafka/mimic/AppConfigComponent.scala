package eu.inn.kafka.mimic

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import eu.inn.kafka.mimic.tools.Utils
import eu.inn.util.ConfigComponent
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConversions._
import scala.util.matching.Regex

case class ZkConfig(
                     connectionString: String,
                     connectionTimeout: Int,
                     sessionTimeout: Int
                   )

case class AppConfig(
                      sourceZkConfig: ZkConfig,
                      targetZkConfig: ZkConfig,
                      consumerConfig: Properties,
                      producerConfig: Properties,
                      whitelist: Seq[Regex],
                      blacklist: Seq[Regex],
                      prefix: String,
                      partitioner: Partitioner,
                      replicationFactor: Int,
                      workersCount: Int,
                      batchSize: Int,
                      skipCorrupted: Boolean
                    )

trait AppConfigComponent {

  this: ConfigComponent ⇒

  val appConfig = {
    val conf = config.getConfig("inn.kafka-mimic")
    AppConfig(
      buildZkConfig(conf.getConfig("source.zookeeper")),
      buildZkConfig(conf.getConfig("target.zookeeper")),
      buildConsumerConfig(conf),
      buildProducerConfig(conf),
      conf.getStringList("source.whitelist").map(_.r),
      conf.getStringList("source.blacklist").map(_.r),
      conf.getString("target.prefix"),
      resolvePartitioner(conf.getString("target.partitioning")),
      conf.getInt("target.replication-factor"),
      conf.getInt("workers"),
      conf.getInt("batch.size"),
      conf.getBoolean("source.skip-corrupted")
    )
  }

  private def buildZkConfig(config: Config) = {
    ZkConfig(
      config.getString("connect"),
      config.getInt("connection.timeout.ms"),
      config.getInt("session.timeout.ms")
    )
  }

  private def buildConsumerConfig(config: Config) = {
    val consumerProps = Utils.buildProperties(
      config
        .getConfig("source.consumer")
        .withFallback(ConfigFactory.parseMap(Map(
          "group.id" → "mimic",
          "auto.offset.reset" → "smallest"
        )))
    )

    consumerProps.put("auto.commit.enable", "false")
    consumerProps.put("consumer.timeout.ms", "100")

    consumerProps.putAll(Utils.buildProperties(config.getConfig("source.zookeeper").atKey("zookeeper")))
    consumerProps
  }

  private def buildProducerConfig(config: Config) = {
    Utils.buildProperties(config.getConfig("target.producer")
      .withFallback(ConfigFactory.parseMap(Map(
        ProducerConfig.COMPRESSION_TYPE_CONFIG → "none",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG → classOf[ByteArraySerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG → classOf[ByteArraySerializer].getName,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION → "1"
      ))))
  }

  private def resolvePartitioner(name: String) =
    name match {
      case "preserve-partition" ⇒ new KeepingPartitionPartitioner
      case "preserve-ordering" ⇒ new KeepingOrderingPartitioner
      case "samza-friendly" ⇒ new SamzaFriendlyPartitioner
      case "random" ⇒ new RandomPartitioner
      case _ ⇒ throw new Exception("Unknown partitioning: " + name)
    }
}