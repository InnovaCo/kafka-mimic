package eu.inn.kafka.mimic

import eu.inn.util.ConfigComponent
import eu.inn.util.metrics.StatsComponent

trait ComponentRegistry
  extends ConfigComponent
  with AppConfigComponent
  with StatsComponent
  with ActorSystemComponent
  with CoordinatorComponent
  with WorkerComponent
  with PublisherComponent
