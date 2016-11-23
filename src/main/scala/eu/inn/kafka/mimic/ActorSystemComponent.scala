package eu.inn.kafka.mimic

import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext

trait ActorSystemComponent {
  val actorSystem = ActorSystem("kafka-mimic")

  implicit val executionContext: ExecutionContext = actorSystem.dispatcher
}

