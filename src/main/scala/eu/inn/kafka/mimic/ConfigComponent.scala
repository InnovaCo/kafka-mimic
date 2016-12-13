package eu.inn.kafka.mimic

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import eu.inn.kafka.mimic.tools.Utils
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

object ConfigComponent {
  val configPath = Option(System.getProperty("config.path"))
  val config = configPath.map(c ⇒ ConfigFactory.parseFile(new File(c))).getOrElse(ConfigFactory.empty())
      .withFallback(ConfigFactory.load())
      .getConfig("inn.kafka-mimic")
}

trait ConfigComponent {

  val config = ConfigComponent.config

  val appConfig = {
    AppConfig(
      buildZkConfig(config.getConfig("source.zookeeper")),
      buildZkConfig(config.getConfig("target.zookeeper")),
      buildConsumerConfig(config),
      buildProducerConfig(config),
      config.getStringList("source.whitelist").map(_.r),
      config.getStringList("source.blacklist").map(_.r),
      config.getString("target.prefix"),
      resolvePartitioner(config.getString("target.partitioning")),
      config.getInt("target.replication-factor"),
      config.getInt("workers"),
      config.getInt("batch.size"),
      config.getBoolean("source.skip-corrupted")
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
