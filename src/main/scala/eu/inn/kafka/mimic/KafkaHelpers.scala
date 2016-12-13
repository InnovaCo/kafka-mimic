package eu.inn.kafka.mimic

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConversions._
import scala.util.matching.Regex

final case class TopicDetails(replicationFactor: Int, partitions: Int, config: Map[String, String])

trait KafkaHelpers {
  self: Logging ⇒

  def buildZkClient(config: ZkConfig) = {
    new ZkClient(
      config.connectionString,
      config.connectionTimeout,
      config.sessionTimeout,
      ZKStringSerializer
    )
  }

  def buildTopicsMap(zk: ZkClient, whitelist: Seq[Regex], blacklist: Seq[Regex], prefix: String): Map[String, String] = {
    ZkUtils.getAllTopics(zk)
      .filter(t ⇒ whitelist.exists(r ⇒ r.findFirstIn(t).nonEmpty) && blacklist.forall(r ⇒ r.findFirstIn(t).isEmpty))
      .map(t ⇒ t → (prefix + t))
      .toMap
  }

  private def describeTopic(topic: String, zk: ZkClient): Option[TopicDetails] = {
    ZkUtils.getPartitionsForTopics(zk, topic :: Nil).headOption match {
      case Some((_, partitions)) if partitions.nonEmpty ⇒
        val replicationFactor = ZkUtils.getReplicasForPartition(zk, topic, 0).size
        Some(TopicDetails(replicationFactor, partitions.size, AdminUtils.fetchTopicConfig(zk, topic).toMap))
      case _ ⇒ None
    }
  }

  def syncTopicConfig(sourceTopic: String, sourceZk: ZkClient, targetTopic: String, targetZk: ZkClient, keepPartition: Boolean, replicationFactor: Int) = {
    val s = describeTopic(sourceTopic, sourceZk).getOrElse(throw new Exception(s"Unable to get source topic details for: $sourceTopic"))
    val sourceConfig = new Properties()
    sourceConfig.putAll(s.config)

    describeTopic(targetTopic, targetZk) match {
      case Some(t) ⇒

        if (s.partitions > t.partitions && keepPartition) {
          log.info(s"$sourceTopic => $targetTopic: found, adding partitions, from ${t.partitions} to ${s.partitions}.")
          AdminUtils.addPartitions(targetZk, targetTopic, s.partitions)
        }

        val configInSync = s.config.forall { case(k, v) ⇒ t.config.get(k).contains(v) }
        if (configInSync) {
          log.info(s"$sourceTopic => $targetTopic: found, config in sync: $s == $t.")
        } else {
          log.info(s"$sourceTopic => $targetTopic: found, overriding config: $s => $t.")
          AdminUtils.changeTopicConfig(targetZk, targetTopic, sourceConfig)
        }
      case None ⇒
        log.info(s"$sourceTopic => $targetTopic: not found, creating new with settings from $s.")
        val rf = if (replicationFactor > 0) replicationFactor else s.replicationFactor
        AdminUtils.createTopic(targetZk, targetTopic, s.partitions, rf, sourceConfig)
    }
  }
}
