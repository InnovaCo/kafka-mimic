package eu.inn.kafka

import akka.actor.ActorRef

package object mimic {

  case object Start

  case object RequestShutdown
  case object Shutdown

  case object RequestSync
  case object Sync
  case object SyncCompleted

  case object RequestReassign
  case object Reassign
  case class Assignment(job: Job)

  case object NextBatch
  case object BatchCompleted

  case class Error(throwable: Throwable, actor: ActorRef)
}
