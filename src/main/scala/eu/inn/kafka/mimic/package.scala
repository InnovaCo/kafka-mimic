package eu.inn.kafka

import akka.actor.ActorRef
import kafka.javaapi.consumer.ConsumerConnector
import org.I0Itec.zkclient.ZkClient

package object mimic {

  object Start

  object NotConnected
  final case class Connected(sourceZk: ZkClient, targetZk: ZkClient, connector: ConsumerConnector)

  object RequestShutdown
  object Shutdown

  object RequestSync
  object Sync
  object SyncCompleted

  object RequestReassign
  object Reassign
  final case class Assignment(job: Job)

  object NextBatch
  object BatchCompleted

  final case class Error(throwable: Throwable, actor: ActorRef)
}
