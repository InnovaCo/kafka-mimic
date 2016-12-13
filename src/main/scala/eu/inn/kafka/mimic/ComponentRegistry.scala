package eu.inn.kafka.mimic

trait ComponentRegistry
  extends ConfigComponent
  with MetricsComponent
  with ActorSystemComponent
  with CoordinatorComponent
  with WorkerComponent
  with PublisherComponent
